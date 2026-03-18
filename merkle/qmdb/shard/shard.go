// Package shard implements one of the 16 parallel Shards in QMDB.
//
// A Shard owns:
//   - A B-tree in-memory index mapping 9-byte key prefix → current Entry ID.
//   - An append-only CSV log (simulating SSD sequential writes).
//   - The current Fresh Twig (receiving new Entry hashes).
//   - A map of all Full/Inactive Twigs (288 bytes each).
//
// All public methods on Shard are safe for concurrent use (protected by mu).
// The upper-tree integration and CSV observability are handled by the db layer.
package shard

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/qmdb/crypto"
	"github.com/qmdb/twig"
	"github.com/qmdb/types"
)

// RootChangeCallback is called whenever a Shard's Merkle root changes (after each write).
// The db layer uses this to update the upper Merkle tree.
type RootChangeCallback func(shardID int, twigID uint64, newTwigRoot crypto.Hash)

// Shard encapsulates all state for one of the 16 QMDB shards.
type Shard struct {
	mu sync.RWMutex

	// shardID is 0..15, derived from the first nibble of the hashed key.
	shardID int

	// index is the in-memory B-tree: 9-byte prefix → Entry ID of current version.
	index *BTreeIndex

	// log is the CSV append file (simulates NVMe SSD for this Shard).
	log *AppendLog

	// freshTwig is the Twig currently receiving new entries. Always exactly one per Shard.
	freshTwig *twig.Twig

	// twigs stores all Full and Inactive Twigs (288 B each) by TwigID.
	// Fresh Twig is in freshTwig, not here.
	twigs map[uint64]*twig.Twig

	// nextEntryID is the global entry ID counter for this Shard.
	// Real QMDB uses a global counter; here it is per-Shard for simplicity.
	nextEntryID uint64

	// onRootChange is called after every write that updates the Twig root.
	// The upper tree hooks in here.
	onRootChange RootChangeCallback

	// nextTwigID is the TwigID that will be assigned to the next Fresh Twig.
	nextTwigID uint64

	// totalEntryCount counts all entries ever appended (including superseded ones).
	totalEntryCount int64
}

// NewShard creates a new empty Shard.
// dataDir is the directory where the CSV append log file will be created.
func NewShard(shardID int, dataDir string, onRootChange RootChangeCallback) (*Shard, error) {
	logPath := fmt.Sprintf("%s/entries_shard_%d.csv", dataDir, shardID)
	al, err := NewAppendLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("shard %d: %w", shardID, err)
	}

	s := &Shard{
		shardID:      shardID,
		index:        NewBTreeIndex(),
		log:          al,
		twigs:        make(map[uint64]*twig.Twig),
		onRootChange: onRootChange,
	}

	// Create the initial Fresh Twig (TwigID = 0).
	s.freshTwig = twig.NewFreshTwig(0, shardID)
	s.nextTwigID = 1

	// Seed the ordered linked list with MIN and MAX sentinel entries.
	if al.RowCount() == 0 {
		if err := s.insertSentinels(); err != nil {
			return nil, fmt.Errorf("shard %d insert sentinels: %w", shardID, err)
		}
	}

	return s, nil
}

// ──────────────────────────── Public read operations ─────────────────────────

// Get returns the current value for the given 28-byte hashed key.
// Returns nil if the key does not exist (was deleted or never inserted).
// Costs: 1 B-tree lookup (memory) + 1 CSV seek-read (simulated disk).
func (s *Shard) Get(key [types.KeySize]byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entryID, found := s.index.Lookup(key)
	if !found {
		return nil, nil
	}

	e, err := s.log.ReadEntry(entryID)
	if err != nil {
		return nil, err
	}

	// Validate full key to handle prefix collisions (extremely rare).
	if e.Key != key {
		return nil, nil
	}
	if e.IsDeleted {
		return nil, nil
	}
	return e.Value, nil
}

// GetEntry returns the raw Entry (including metadata) for the given key.
// Returns nil if not found.
func (s *Shard) GetEntry(key [types.KeySize]byte) (*types.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getEntryLocked(key)
}

// GetAtVersion retrieves the value of a key as it was at a specific Version.
// Traverses the OldId chain backwards until finding the entry whose Version ≤ target
// and whose next-version Version > target.
func (s *Shard) GetAtVersion(key [types.KeySize]byte, targetVersion types.Version) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entryID, found := s.index.Lookup(key)
	if !found {
		// Key might have been deleted; we still need to walk history.
		// For simplicity, return nil for now (production would need a separate deleted-key index).
		return nil, nil
	}

	// Walk the OldId chain to find the version at targetVersion.
	for entryID != types.NullEntryID {
		e, err := s.log.ReadEntry(entryID)
		if err != nil {
			return nil, err
		}
		if e.Version <= targetVersion {
			if e.IsDeleted {
				return nil, nil
			}
			return e.Value, nil
		}
		entryID = e.OldId
	}
	return nil, nil
}

// ──────────────────────────── Public write operations ────────────────────────

// Insert creates a new key-value pair. The key must not already exist.
// Maintenance of the ordered NextKey linked list requires:
//  1. Reading the predecessor entry (1 CSV read).
//  2. Appending a new Entry for the new key.
//  3. Appending a new version of the predecessor (updated NextKey).
func (s *Shard) Insert(key [types.KeySize]byte, value []byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, found := s.index.Lookup(key); found {
		return fmt.Errorf("Insert: key %x already exists; use Update", key)
	}

	// Find the predecessor in the ordered list (the largest active key < newKey).
	predKey, predEntryID, err := s.findPredecessor(key)
	if err != nil {
		return fmt.Errorf("Insert findPredecessor: %w", err)
	}

	// Read the predecessor entry to get its current NextKey.
	predEntry, err := s.log.ReadEntry(predEntryID)
	if err != nil {
		return fmt.Errorf("Insert read predecessor: %w", err)
	}

	// ── Step 1: Append the new key's Entry ──────────────────────────────────
	newEntry := &types.Entry{
		Id:           s.nextEntryID,
		Key:          key,
		Value:        append([]byte{}, value...),
		NextKey:      predEntry.NextKey, // new entry's successor = predecessor's old successor
		OldId:        types.NullEntryID,
		OldNextKeyId: types.NullEntryID,
		Version:      version,
		IsDeleted:    false,
	}
	if _, err := s.appendEntry(newEntry); err != nil {
		return err
	}

	// ── Step 2: Append a new version of the predecessor ─────────────────────
	// The predecessor's NextKey changes from its old value to the new key.
	updatedPred := &types.Entry{
		Id:           s.nextEntryID,
		Key:          predKey,
		Value:        append([]byte{}, predEntry.Value...),
		NextKey:      key, // now points to the new key
		OldId:        predEntryID,
		OldNextKeyId: predEntry.OldNextKeyId,
		Version:      version,
		IsDeleted:    predEntry.IsDeleted,
	}
	if _, err := s.appendEntry(updatedPred); err != nil {
		return err
	}

	// ── Update index ────────────────────────────────────────────────────────
	// Mark predecessor's old entry inactive in its Twig.
	s.markEntryInactive(predEntryID)
	// Index: new key → new entry; predecessor → updated predecessor.
	s.index.Upsert(key, newEntry.Id)
	s.index.Upsert(predKey, updatedPred.Id)

	return nil
}

// Update modifies the value of an existing key.
// Appends exactly 1 new Entry; NextKey is preserved.
func (s *Shard) Update(key [types.KeySize]byte, value []byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentID, found := s.index.Lookup(key)
	if !found {
		return fmt.Errorf("Update: key %x not found; use Insert", key)
	}

	currentEntry, err := s.log.ReadEntry(currentID)
	if err != nil {
		return err
	}
	if currentEntry.IsDeleted {
		return fmt.Errorf("Update: key %x is deleted", key)
	}

	newEntry := &types.Entry{
		Id:           s.nextEntryID,
		Key:          key,
		Value:        append([]byte{}, value...),
		NextKey:      currentEntry.NextKey,
		OldId:        currentID,
		OldNextKeyId: currentEntry.OldNextKeyId,
		Version:      version,
		IsDeleted:    false,
	}
	if _, err := s.appendEntry(newEntry); err != nil {
		return err
	}

	s.markEntryInactive(currentID)
	s.index.Upsert(key, newEntry.Id)
	return nil
}

// Delete removes a key from the active state.
// Appends a tombstone Entry and updates the predecessor's NextKey.
// Costs: 2 CSV reads (predecessor + deleted key) + 1 append.
func (s *Shard) Delete(key [types.KeySize]byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentID, found := s.index.Lookup(key)
	if !found {
		return fmt.Errorf("Delete: key %x not found", key)
	}

	currentEntry, err := s.log.ReadEntry(currentID)
	if err != nil {
		return err
	}
	if currentEntry.IsDeleted {
		return fmt.Errorf("Delete: key %x already deleted", key)
	}

	// Find the predecessor.
	predKey, predEntryID, err := s.findPredecessor(key)
	if err != nil {
		return fmt.Errorf("Delete findPredecessor: %w", err)
	}

	predEntry, err := s.log.ReadEntry(predEntryID)
	if err != nil {
		return err
	}

	// Append updated predecessor: skip over the deleted key.
	updatedPred := &types.Entry{
		Id:           s.nextEntryID,
		Key:          predKey,
		Value:        append([]byte{}, predEntry.Value...),
		NextKey:      currentEntry.NextKey, // jump over the deleted key
		OldId:        predEntryID,
		OldNextKeyId: currentID, // OldNextKeyId points to the entry that *had* this relationship
		Version:      version,
		IsDeleted:    predEntry.IsDeleted,
	}
	if _, err := s.appendEntry(updatedPred); err != nil {
		return err
	}

	// Mark both old entries inactive.
	s.markEntryInactive(currentID)
	s.markEntryInactive(predEntryID)

	// Remove the deleted key from the index; update predecessor.
	s.index.Delete(key)
	s.index.Upsert(predKey, updatedPred.Id)
	return nil
}

// ──────────────────────────── Twig / root queries ────────────────────────────

// FreshTwigRoot returns the current root hash of the Fresh Twig.
func (s *Shard) FreshTwigRoot() crypto.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.freshTwig.RootHash
}

// FreshTwig returns a read-only view of the current Fresh Twig.
func (s *Shard) FreshTwigSnapshot() *twig.Twig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.freshTwig
}

// AllTwigs returns a snapshot of all Full/Inactive twigs (not the Fresh one).
func (s *Shard) AllTwigs() map[uint64]*twig.Twig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[uint64]*twig.Twig, len(s.twigs))
	for k, v := range s.twigs {
		snap[k] = v
	}
	return snap
}

// GetEntryByID reads an Entry by its global ID from the CSV append log.
// Used by the Compaction worker to read entries in a Full Twig.
func (s *Shard) GetEntryByID(entryID uint64) (*types.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.log.ReadEntry(entryID)
}

// IndexSnapshot returns a copy of all B-tree index entries for observability.
func (s *Shard) IndexSnapshot() []IndexEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var entries []IndexEntry
	s.index.Ascend(func(e IndexEntry) bool {
		entries = append(entries, e)
		return true
	})
	return entries
}

// NextKeyChain reconstructs the ordered linked list from the index.
// Returns a slice of [key_hex, entry_id] pairs in sorted order.
func (s *Shard) NextKeyChain() ([][2]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Start from MIN sentinel.
	minID, found := s.index.Lookup(types.MinKey)
	if !found {
		return nil, fmt.Errorf("MIN sentinel not found in shard %d", s.shardID)
	}

	var chain [][2]string
	currentID := minID
	visited := make(map[uint64]bool)

	for {
		if visited[currentID] {
			break
		}
		visited[currentID] = true

		e, err := s.log.ReadEntry(currentID)
		if err != nil {
			return nil, err
		}

		chain = append(chain, [2]string{
			fmt.Sprintf("%x", e.Key),
			fmt.Sprintf("%d", e.Id),
		})

		if e.NextKey == types.MaxKey {
			// Append MAX sentinel.
			chain = append(chain, [2]string{
				fmt.Sprintf("%x", types.MaxKey),
				"MAX_SENTINEL",
			})
			break
		}

		// Look up next key in index.
		nextID, ok := s.index.Lookup(e.NextKey)
		if !ok {
			break
		}
		currentID = nextID
	}
	return chain, nil
}

// ──────────────────────────── internal helpers ────────────────────────────────

// insertSentinels writes the MIN and MAX sentinel entries at Shard initialisation.
// MIN.NextKey = MAX, forming an empty ordered list.
func (s *Shard) insertSentinels() error {
	// MAX sentinel (no successors; it's the end of the list).
	maxEntry := &types.Entry{
		Id:           s.nextEntryID,
		Key:          types.MaxKey,
		Value:        nil,
		NextKey:      types.MaxKey, // MAX points to itself (end marker)
		OldId:        types.NullEntryID,
		OldNextKeyId: types.NullEntryID,
		Version:      types.Version(0),
		IsDeleted:    false,
	}
	if _, err := s.appendEntry(maxEntry); err != nil {
		return err
	}
	s.index.Upsert(types.MaxKey, maxEntry.Id)

	// MIN sentinel: NextKey = MAX.
	minEntry := &types.Entry{
		Id:           s.nextEntryID,
		Key:          types.MinKey,
		Value:        nil,
		NextKey:      types.MaxKey,
		OldId:        types.NullEntryID,
		OldNextKeyId: types.NullEntryID,
		Version:      types.Version(0),
		IsDeleted:    false,
	}
	if _, err := s.appendEntry(minEntry); err != nil {
		return err
	}
	s.index.Upsert(types.MinKey, minEntry.Id)
	return nil
}

// appendEntry writes an entry to the CSV log, hashes it into the Fresh Twig,
// and returns the byte offset. Calls onRootChange if the Twig root changes.
// MUST be called with s.mu held (write lock).
func (s *Shard) appendEntry(e *types.Entry) (int64, error) {
	e.Id = s.nextEntryID
	s.nextEntryID++
	s.totalEntryCount++

	offset, err := s.log.Append(e)
	if err != nil {
		return 0, err
	}

	// Hash the entry and append it as a leaf in the Fresh Twig.
	leafHash := crypto.HashEntry(
		e.Id, e.Key[:], e.Value, e.NextKey[:],
		e.OldId, e.OldNextKeyId,
		uint64(e.Version), e.IsDeleted,
	)
	newRoot := s.freshTwig.AppendLeaf(leafHash)

	// Notify upper tree of the new root.
	if s.onRootChange != nil {
		s.onRootChange(s.shardID, s.freshTwig.TwigID, newRoot)
	}

	// If the Fresh Twig just became full, transition it to Full and open a new one.
	if s.freshTwig.IsFull() {
		s.sealFreshTwig()
	}

	return offset, nil
}

// sealFreshTwig transitions the current Fresh Twig to Full and creates a new Fresh Twig.
func (s *Shard) sealFreshTwig() {
	old := s.freshTwig
	old.TransitionToFull()
	s.twigs[old.TwigID] = old

	// Open a new Fresh Twig.
	s.freshTwig = twig.NewFreshTwig(s.nextTwigID, s.shardID)
	s.nextTwigID++

	// Notify upper tree of the empty new twig's root.
	if s.onRootChange != nil {
		s.onRootChange(s.shardID, s.freshTwig.TwigID, s.freshTwig.RootHash)
	}
}

// markEntryInactive clears the ActiveBit for the given entry in its Twig.
// For entries in the Fresh Twig (which hasn't been sealed yet), inactivation
// is deferred to the Full state — the Fresh Twig always has all written slots considered active.
// For entries in Full Twigs, we set ActiveBits immediately.
func (s *Shard) markEntryInactive(entryID uint64) {
	twigID := entryID / types.TwigSize
	slot := int(entryID % types.TwigSize)

	if twigID == s.freshTwig.TwigID {
		// Entry is in the Fresh Twig; ActiveBits are managed on transition to Full.
		// We record a "to-deactivate" list, but for simplicity in this implementation,
		// we handle it in the Full Twig after sealing.
		return
	}

	t, ok := s.twigs[twigID]
	if !ok {
		return
	}
	t.MarkSlotInactive(slot)
}

// findPredecessor locates the largest active key strictly less than `key`
// in the ordered linked list. Since the index only contains 9-byte prefixes,
// we use the B-tree DescendLessOrEqual to find the floor entry, then read it
// from CSV to confirm the full key match.
func (s *Shard) findPredecessor(key [types.KeySize]byte) ([types.KeySize]byte, uint64, error) {
	predKey, predID, found := s.index.FindPredecessor(key)
	if !found {
		// Should always find MIN sentinel since it's always in the index.
		return types.MinKey, 0, fmt.Errorf("no predecessor found for key %x (missing MIN sentinel?)", key)
	}

	// Verify by reading the candidate entry.
	e, err := s.log.ReadEntry(predID)
	if err != nil {
		return types.MinKey, 0, err
	}

	// Confirm it's an active entry whose Key < target key.
	if bytes.Compare(e.Key[:], key[:]) >= 0 {
		return types.MinKey, 0, fmt.Errorf("predecessor check failed: %x >= %x", e.Key, key)
	}

	return predKey, predID, nil
}

// getEntryLocked reads the current Entry for a key from the log.
// MUST be called with at least s.mu.RLock held.
func (s *Shard) getEntryLocked(key [types.KeySize]byte) (*types.Entry, error) {
	entryID, found := s.index.Lookup(key)
	if !found {
		return nil, nil
	}
	return s.log.ReadEntry(entryID)
}
