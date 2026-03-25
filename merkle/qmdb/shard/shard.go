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

// TraceHook is an optional fine-grained observability callback.
// Each sub-step inside an Insert/Update/Delete operation calls this with a short
// machine-readable subOp string and a human-readable detail string.
// All string arguments are pre-formatted by the caller (hex-encoded, etc.).
type TraceHook func(op, subOp, keyHex, entryID, oldID, twigID, slot, block, tx, hashHex, detail string)

// deletedKeyRecord holds the last-known entry ID and deletion version for a deleted key.
// Used by GetAtVersion to walk history for keys that have been removed.
type deletedKeyRecord struct {
	lastEntryID     uint64
	deletionVersion types.Version
}

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

	// twigs stores all Full twigs (288 B each) by TwigID.
	// Inactive twigs are promoted to prunedTwigRoots and removed from here.
	twigs map[uint64]*twig.Twig

	// prunedTwigRoots holds the root hash of twigs that have been pruned from memory
	// (all entries superseded). The root hash is still needed by the UpperTree.
	prunedTwigRoots map[uint64]crypto.Hash

	// nextEntryID is the global entry ID counter for this Shard.
	nextEntryID uint64

	// onRootChange is called after every write that updates the Twig root.
	onRootChange RootChangeCallback

	// nextTwigID is the TwigID that will be assigned to the next Fresh Twig.
	nextTwigID uint64

	// totalEntryCount counts all entries ever appended (including superseded ones).
	totalEntryCount int64

	// freshInactiveSlots records slot indices in the current Fresh Twig that have been
	// superseded by a newer entry. Applied when the Fresh Twig is sealed.
	freshInactiveSlots []int

	// deletedKeys maps deleted storage keys to their last entry ID + deletion version.
	deletedKeys map[[types.KeySize]byte]deletedKeyRecord

	// lifecycleBreaks records deletion events for keys that have since been re-inserted.
	// Needed to answer GetAtVersion queries that fall within a "deleted window" of a
	// multi-lifecycle key (insert → delete → re-insert).
	lifecycleBreaks map[[types.KeySize]byte][]deletedKeyRecord

	// traceHook is an optional fine-grained trace callback (nil = disabled).
	traceHook TraceHook

	// currentOp is the high-level operation name for the current write ("Set"/"Delete").
	currentOp string

	// currentBlock / currentTx are forwarded from db for trace annotations.
	currentBlock uint64
	currentTx    uint32
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
		shardID:         shardID,
		index:           NewBTreeIndex(),
		log:             al,
		twigs:           make(map[uint64]*twig.Twig),
		prunedTwigRoots: make(map[uint64]crypto.Hash),
		onRootChange:    onRootChange,
		deletedKeys:     make(map[[types.KeySize]byte]deletedKeyRecord),
		lifecycleBreaks: make(map[[types.KeySize]byte][]deletedKeyRecord),
	}

	// Create the initial Fresh Twig (TwigID = 0).
	s.freshTwig = twig.NewFreshTwig(0, shardID)
	s.nextTwigID = 1

	if al.RowCount() == 0 {
		// Brand-new shard: seed with MIN/MAX sentinels.
		if err := s.insertSentinels(); err != nil {
			return nil, fmt.Errorf("shard %d insert sentinels: %w", shardID, err)
		}
	} else {
		// Existing data: rebuild all in-memory state from the CSV log.
		if err := s.rebuildFromLog(); err != nil {
			return nil, fmt.Errorf("shard %d rebuild: %w", shardID, err)
		}
	}

	return s, nil
}

// SetTraceHook attaches a fine-grained trace callback to this Shard.
// Pass nil to disable tracing.
func (s *Shard) SetTraceHook(h TraceHook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traceHook = h
}

// SetCurrentOp is called by the db layer just before a write to annotate traces.
func (s *Shard) SetCurrentOp(op string, block uint64, tx uint32) {
	s.currentOp = op
	s.currentBlock = block
	s.currentTx = tx
}

// trace emits one sub-step row if a TraceHook is installed.
// MUST be called with s.mu held (read or write).
func (s *Shard) trace(subOp, keyHex, entryID, oldID, twigID, slot, hashHex, detail string) {
	if s.traceHook == nil {
		return
	}
	s.traceHook(
		s.currentOp, subOp,
		keyHex, entryID, oldID, twigID, slot,
		fmt.Sprintf("%d", s.currentBlock),
		fmt.Sprintf("%d", s.currentTx),
		hashHex, detail,
	)
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
		// Key was deleted: check the deleted-key index for historical lookups.
		rec, deleted := s.deletedKeys[key]
		if !deleted {
			return nil, nil
		}
		// If the target version is at or after deletion, the key didn't exist.
		if targetVersion >= rec.deletionVersion {
			return nil, nil
		}
		entryID = rec.lastEntryID
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
			// Before returning, check whether this key was deleted during the
			// window (e.Version, targetVersion]. This handles multi-lifecycle keys
			// where the OldId chain skips over a deletion event.
			for _, br := range s.lifecycleBreaks[key] {
				if e.Version < br.deletionVersion && br.deletionVersion <= targetVersion {
					return nil, nil
				}
			}
			return e.Value, nil
		}
		entryID = e.OldId
	}
	return nil, nil
}

// ──────────────────────────── Public write operations ────────────────────────

// Insert creates a new key-value pair. The key must not already exist.
func (s *Shard) Insert(key [types.KeySize]byte, value []byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.insertLocked(key, value, version)
}

// insertLocked is the implementation of Insert; caller must hold s.mu write lock.
func (s *Shard) insertLocked(key [types.KeySize]byte, value []byte, version types.Version) error {
	keyHex := fmt.Sprintf("%x", key)

	// ① B-Tree lookup: check key does not exist
	_, found := s.index.Lookup(key)
	s.trace("btree_lookup", keyHex, "", "", "", "", "", fmt.Sprintf("key %s → found=%v (Insert: must be absent)", keyHex[:16]+"…", found))
	if found {
		return fmt.Errorf("Insert: key %x already exists; use Update", key)
	}

	// ② Find predecessor in sorted linked list
	predKey, predEntryID, err := s.findPredecessor(key)
	if err != nil {
		return fmt.Errorf("Insert findPredecessor: %w", err)
	}
	s.trace("find_predecessor", keyHex, fmt.Sprintf("%d", predEntryID), "", "", "", "", fmt.Sprintf("predecessor key=%x… entry_id=%d", predKey[:4], predEntryID))

	predEntry, err := s.log.ReadEntry(predEntryID)
	if err != nil {
		return fmt.Errorf("Insert read predecessor: %w", err)
	}

	// If this key was previously deleted, chain the OldId to preserve full history.
	oldID := types.NullEntryID
	if rec, deleted := s.deletedKeys[key]; deleted {
		oldID = rec.lastEntryID
		delete(s.deletedKeys, key)
		s.lifecycleBreaks[key] = append(s.lifecycleBreaks[key], rec)
	}

	newEntry := &types.Entry{
		Key:          key,
		Value:        append([]byte{}, value...),
		NextKey:      predEntry.NextKey,
		OldId:        oldID,
		OldNextKeyId: types.NullEntryID,
		Version:      version,
	}
	// ③ Append new Entry (the actual key's data)
	if _, err := s.appendEntry(newEntry); err != nil {
		return err
	}

	updatedPred := &types.Entry{
		Key:          predKey,
		Value:        append([]byte{}, predEntry.Value...),
		NextKey:      key,
		OldId:        predEntryID,
		OldNextKeyId: predEntry.OldNextKeyId,
		Version:      version,
		IsDeleted:    predEntry.IsDeleted,
	}
	// ④ Append updated predecessor (NextKey now points to new entry)
	if _, err := s.appendEntry(updatedPred); err != nil {
		return err
	}

	// ⑤ Mark old predecessor inactive
	s.markEntryInactive(predEntryID)
	s.trace("mark_inactive", fmt.Sprintf("%x", predKey), fmt.Sprintf("%d", predEntryID), "", fmt.Sprintf("%d", predEntryID/uint64(types.TwigSize)), fmt.Sprintf("%d", predEntryID%uint64(types.TwigSize)), "", fmt.Sprintf("predecessor entry #%d superseded by #%d", predEntryID, updatedPred.Id))

	// ⑥ Update B-Tree index
	s.index.Upsert(key, newEntry.Id)
	s.trace("btree_update", keyHex, fmt.Sprintf("%d", newEntry.Id), "", "", "", "", fmt.Sprintf("index[key=%s…] ← entry_id=%d (new entry)", keyHex[:16]+"…", newEntry.Id))
	s.index.Upsert(predKey, updatedPred.Id)
	s.trace("btree_update", fmt.Sprintf("%x", predKey), fmt.Sprintf("%d", updatedPred.Id), fmt.Sprintf("%d", predEntryID), "", "", "", fmt.Sprintf("index[predKey=%x…] ← entry_id=%d (updated predecessor)", predKey[:4], updatedPred.Id))
	return nil
}

// Update modifies the value of an existing key.
func (s *Shard) Update(key [types.KeySize]byte, value []byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(key, value, version)
}

// updateLocked is the implementation of Update; caller must hold s.mu write lock.
func (s *Shard) updateLocked(key [types.KeySize]byte, value []byte, version types.Version) error {
	keyHex := fmt.Sprintf("%x", key)

	// ① B-Tree lookup: find current entry
	currentID, found := s.index.Lookup(key)
	s.trace("btree_lookup", keyHex, fmt.Sprintf("%d", currentID), "", "", "", "", fmt.Sprintf("key %s… → entry_id=%d (Update: must exist)", keyHex[:16]+"…", currentID))
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
		Key:          key,
		Value:        append([]byte{}, value...),
		NextKey:      currentEntry.NextKey,
		OldId:        currentID,
		OldNextKeyId: currentEntry.OldNextKeyId,
		Version:      version,
	}
	// ② Append new version of Entry (OldId → previous version)
	if _, err := s.appendEntry(newEntry); err != nil {
		return err
	}
	// ③ Mark old version inactive
	s.markEntryInactive(currentID)
	s.trace("mark_inactive", keyHex, fmt.Sprintf("%d", currentID), "", fmt.Sprintf("%d", currentID/uint64(types.TwigSize)), fmt.Sprintf("%d", currentID%uint64(types.TwigSize)), "", fmt.Sprintf("entry #%d superseded by new entry #%d", currentID, newEntry.Id))
	// ④ Update B-Tree index
	s.index.Upsert(key, newEntry.Id)
	s.trace("btree_update", keyHex, fmt.Sprintf("%d", newEntry.Id), fmt.Sprintf("%d", currentID), "", "", "", fmt.Sprintf("index[key=%s…] ← entry_id=%d (was %d)", keyHex[:16]+"…", newEntry.Id, currentID))
	return nil
}

// Upsert atomically inserts or updates a key-value pair under a single write lock,
// eliminating the TOCTOU race that would occur with a separate Get + Insert/Update.
func (s *Shard) Upsert(key [types.KeySize]byte, value []byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.index.Lookup(key); found {
		return s.updateLocked(key, value, version)
	}
	return s.insertLocked(key, value, version)
}

// Delete removes a key from the active state.
// Appends a tombstone Entry and updates the predecessor's NextKey.
// Costs: 2 CSV reads (predecessor + deleted key) + 1 append.
func (s *Shard) Delete(key [types.KeySize]byte, version types.Version) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyHex := fmt.Sprintf("%x", key)

	// ① B-Tree lookup: find current entry to delete
	currentID, found := s.index.Lookup(key)
	s.trace("btree_lookup", keyHex, fmt.Sprintf("%d", currentID), "", "", "", "", fmt.Sprintf("key %s… → entry_id=%d (Delete: found=%v)", keyHex[:16]+"…", currentID, found))
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

	// ② Find predecessor (to re-stitch the linked list)
	predKey, predEntryID, err := s.findPredecessor(key)
	if err != nil {
		return fmt.Errorf("Delete findPredecessor: %w", err)
	}
	s.trace("find_predecessor", keyHex, fmt.Sprintf("%d", predEntryID), "", "", "", "", fmt.Sprintf("predecessor key=%x… entry_id=%d; will re-stitch NextKey to skip deleted entry", predKey[:4], predEntryID))

	predEntry, err := s.log.ReadEntry(predEntryID)
	if err != nil {
		return err
	}

	// ③ Append updated predecessor: skip over the deleted key.
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

	// Record deletion history before removing from the live index.
	s.deletedKeys[key] = deletedKeyRecord{lastEntryID: currentID, deletionVersion: version}

	// ④ Mark both old entries inactive
	s.markEntryInactive(currentID)
	s.trace("mark_inactive", keyHex, fmt.Sprintf("%d", currentID), "", fmt.Sprintf("%d", currentID/uint64(types.TwigSize)), fmt.Sprintf("%d", currentID%uint64(types.TwigSize)), "", fmt.Sprintf("deleted entry #%d marked inactive (tombstone recorded)", currentID))
	s.markEntryInactive(predEntryID)
	s.trace("mark_inactive", fmt.Sprintf("%x", predKey), fmt.Sprintf("%d", predEntryID), "", fmt.Sprintf("%d", predEntryID/uint64(types.TwigSize)), fmt.Sprintf("%d", predEntryID%uint64(types.TwigSize)), "", fmt.Sprintf("old predecessor entry #%d superseded by #%d", predEntryID, updatedPred.Id))

	// ⑤ Remove deleted key from B-Tree; update predecessor
	s.index.Delete(key)
	s.trace("btree_update", keyHex, "", "", "", "", "", fmt.Sprintf("index.Delete(key=%s…) — key removed from live index", keyHex[:16]+"…"))
	s.index.Upsert(predKey, updatedPred.Id)
	s.trace("btree_update", fmt.Sprintf("%x", predKey), fmt.Sprintf("%d", updatedPred.Id), fmt.Sprintf("%d", predEntryID), "", "", "", fmt.Sprintf("index[predKey=%x…] ← entry_id=%d (NextKey now skips deleted)", predKey[:4], updatedPred.Id))
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

	// Sub-step A: write to append log (simulates SSD sequential write)
	keyHex := fmt.Sprintf("%x", e.Key)
	entryIDStr := fmt.Sprintf("%d", e.Id)
	twigIDStr := fmt.Sprintf("%d", e.Id/uint64(types.TwigSize))
	slotStr := fmt.Sprintf("%d", e.Id%uint64(types.TwigSize))
	oldIDStr := ""
	if e.OldId != types.NullEntryID {
		oldIDStr = fmt.Sprintf("%d", e.OldId)
	}
	deletedLabel := ""
	if e.IsDeleted {
		deletedLabel = " [tombstone]"
	}
	s.trace("log_append", keyHex, entryIDStr, oldIDStr, twigIDStr, slotStr, "",
		fmt.Sprintf("append entry #%d to log: key=%s…%s twig=%s slot=%s", e.Id, keyHex[:16]+"…", deletedLabel, twigIDStr, slotStr))

	offset, err := s.log.Append(e)
	if err != nil {
		return 0, err
	}

	// Sub-step B: compute leaf hash (Keccak256 of full entry fields)
	leafHash := crypto.HashEntry(
		e.Id, e.Key[:], e.Value, e.NextKey[:],
		e.OldId, e.OldNextKeyId,
		uint64(e.Version), e.IsDeleted,
	)
	s.trace("leaf_hash", keyHex, entryIDStr, oldIDStr, twigIDStr, slotStr,
		fmt.Sprintf("%x", leafHash),
		fmt.Sprintf("Hash(entry#%d) → leaf_hash=%x… (Keccak256 of key|value|nextKey|oldId|…)", e.Id, leafHash[:8]))

	// Sub-step C: insert leaf into Fresh Twig → recompute Merkle path
	prevRoot := s.freshTwig.RootHash
	newRoot := s.freshTwig.AppendLeaf(leafHash)
	if newRoot != prevRoot {
		s.trace("twig_rehash", keyHex, entryIDStr, "", twigIDStr, slotStr,
			fmt.Sprintf("%x", newRoot),
			fmt.Sprintf("Twig#%s Merkle path rehashed: slot%s filled → new twig_root=%x…", twigIDStr, slotStr, newRoot[:8]))
	}

	// Sub-step D: notify UpperTree of new Twig root → propagate to global root
	if s.onRootChange != nil {
		s.onRootChange(s.shardID, s.freshTwig.TwigID, newRoot)
		s.trace("twig_root_update", keyHex, entryIDStr, "", twigIDStr, "",
			fmt.Sprintf("%x", newRoot),
			fmt.Sprintf("UpperTree.UpdateTwigRoot(shard=%d, twig=%s, root=%x…) → global state root changes", s.shardID, twigIDStr, newRoot[:8]))
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
	old.TransitionToFull() // sets all ActiveBits to 1

	// Apply deferred inactivations: entries that were superseded while still in this twig.
	for _, slot := range s.freshInactiveSlots {
		old.MarkSlotInactive(slot)
	}
	s.freshInactiveSlots = s.freshInactiveSlots[:0]

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
// For entries still in the Fresh Twig, the slot is deferred to freshInactiveSlots.
// For Full Twigs, we clear the bit immediately; if the twig becomes Inactive, prune it.
func (s *Shard) markEntryInactive(entryID uint64) {
	twigID := entryID / uint64(types.TwigSize)
	slot := int(entryID % uint64(types.TwigSize))

	if twigID == s.freshTwig.TwigID {
		s.freshInactiveSlots = append(s.freshInactiveSlots, slot)
		return
	}

	t, ok := s.twigs[twigID]
	if !ok {
		return
	}
	if became := t.MarkSlotInactive(slot); became {
		// Twig just became Inactive — prune it from memory; UpperTree keeps its root hash.
		rootHash := t.RootHash
		t.TransitionToPruned()
		delete(s.twigs, twigID)
		s.prunedTwigRoots[twigID] = rootHash
	}
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

// GetTwig returns the Full or Inactive Twig with the given ID.
// The current Fresh Twig is not returned by this method; use FreshTwigSnapshot for that.
func (s *Shard) GetTwig(twigID uint64) (*twig.Twig, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.twigs[twigID]
	return t, ok
}

// BuildProofForFullTwig reads all entries for twigID from the append log, rebuilds the
// twig's internal Merkle tree in memory, and returns the 11-sibling proof for `slot`.
// Used for generating proofs on Full/Inactive Twigs (where FreshData has been released).
func (s *Shard) BuildProofForFullTwig(twigID uint64, slot int) ([]crypto.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	leaves, err := s.rebuildTwigLeavesLocked(twigID)
	if err != nil {
		return nil, err
	}

	fd := twig.RebuildFromLeaves(leaves)
	// Construct a temporary Fresh-state Twig backed by the rebuilt FreshData.
	tempTwig := &twig.Twig{
		TwigID:   twigID,
		ShardID:  s.shardID,
		Status:   twig.StatusFresh,
		RootHash: fd.Nodes[1],
		Fresh:    fd,
	}
	return tempTwig.MerkleProof(slot), nil
}

// rebuildTwigLeavesLocked reads all 2048 entry hashes for the given twigID from the log.
// Caller must hold at least s.mu.RLock.
func (s *Shard) rebuildTwigLeavesLocked(twigID uint64) ([types.TwigSize]crypto.Hash, error) {
	var leaves [types.TwigSize]crypto.Hash
	for i := range leaves {
		leaves[i] = crypto.NullHash
	}
	startID := twigID * uint64(types.TwigSize)
	for slot := 0; slot < types.TwigSize; slot++ {
		e, err := s.log.ReadEntry(startID + uint64(slot))
		if err != nil {
			continue // entry absent in this shard's log (e.g. first few IDs belong to sentinels)
		}
		leaves[slot] = crypto.HashEntry(
			e.Id, e.Key[:], e.Value, e.NextKey[:],
			e.OldId, e.OldNextKeyId, uint64(e.Version), e.IsDeleted,
		)
	}
	return leaves, nil
}

// FindPredecessorEntry returns the entry for the largest active key strictly less than key.
// Used to build non-existence proofs (predecessor.Key < query ≤ predecessor.NextKey).
func (s *Shard) FindPredecessorEntry(key [types.KeySize]byte) (*types.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, predID, err := s.findPredecessor(key)
	if err != nil {
		return nil, err
	}
	return s.log.ReadEntry(predID)
}

// Close flushes and closes the underlying CSV append log.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

// EmitTwigRoots calls onRootChange for every known twig (Full, Inactive/Pruned, and Fresh).
// Used after rebuildFromLog() to synchronise the UpperTree with the recovered shard state.
func (s *Shard) EmitTwigRoots() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.onRootChange == nil {
		return
	}
	for twigID, rootHash := range s.prunedTwigRoots {
		s.onRootChange(s.shardID, twigID, rootHash)
	}
	for _, t := range s.twigs {
		s.onRootChange(s.shardID, t.TwigID, t.RootHash)
	}
	s.onRootChange(s.shardID, s.freshTwig.TwigID, s.freshTwig.RootHash)
}

// ──────────────────────────── recovery ─────────────────────────────────────────

// rebuildFromLog scans the CSV append log and reconstructs all in-memory state:
//   - B-tree index (key → current entry ID)
//   - deletedKeys map
//   - Full twigs with correct ActiveBits
//   - Current Fresh Twig with leaf hashes
//
// After NewShard returns, the caller should call EmitTwigRoots() to populate
// the UpperTree (db.Open does this for all shards after the wait group).
func (s *Shard) rebuildFromLog() error {
	entries, err := s.log.ReadAllEntries()
	if err != nil {
		return fmt.Errorf("scan log: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Build fast-lookup map and find maxID.
	entryMap := make(map[uint64]*types.Entry, len(entries))
	var maxID uint64
	for _, e := range entries {
		entryMap[e.Id] = e
		if e.Id > maxID {
			maxID = e.Id
		}
	}
	s.nextEntryID = maxID + 1
	s.totalEntryCount = int64(len(entries))

	// Build superseded set: every entry ID that has a newer version.
	superseded := make(map[uint64]bool, len(entries))
	for _, e := range entries {
		if e.OldId != types.NullEntryID {
			superseded[e.OldId] = true
		}
	}

	// Build deletedByOldNextKey set: entry IDs that are pointed to by OldNextKeyId.
	// These correspond to entries whose keys were deleted (the predecessor was updated
	// to skip over them). Unlike superseded entries, these are NOT replaced by a newer
	// version of the same key — the key simply was removed from the live index.
	deletedByOldNextKey := make(map[uint64]bool, len(entries)/4)
	for _, e := range entries {
		if e.OldNextKeyId != types.NullEntryID {
			deletedByOldNextKey[e.OldNextKeyId] = true
		}
	}

	// Determine the current fresh twig.
	// If the last-written entry was the final slot (slot == TwigSize-1), the inline
	// sealFreshTwig() call created an empty new fresh twig (nextTwigID = that+1).
	lastSlot := maxID % uint64(types.TwigSize)
	lastTwigIDx := maxID / uint64(types.TwigSize)
	var freshTwigID uint64
	if lastSlot == uint64(types.TwigSize)-1 {
		freshTwigID = lastTwigIDx + 1
	} else {
		freshTwigID = lastTwigIDx
	}
	s.nextTwigID = freshTwigID + 1

	// Populate live index.
	// An entry is "live" (current) if:
	//   (a) it is NOT superseded by a newer version (OldId chain), AND
	//   (b) it is NOT a deleted entry (not pointed to by any OldNextKeyId field).
	// Condition (b) handles pure-deletion without re-insertion: the deleted entry's
	// ID appears as OldNextKeyId in the updated predecessor, marking it as "gone".
	liveKeys := make(map[[types.KeySize]byte]uint64, len(entries)/2)
	for _, e := range entries {
		if !superseded[e.Id] && !deletedByOldNextKey[e.Id] {
			liveKeys[e.Key] = e.Id
		}
	}
	for key, id := range liveKeys {
		s.index.Upsert(key, id)
	}

	// Populate freshInactiveSlots: fresh-twig entries already superseded.
	for _, e := range entries {
		if e.TwigID() == freshTwigID && superseded[e.Id] {
			s.freshInactiveSlots = append(s.freshInactiveSlots, int(e.SlotIndex()))
		}
	}

	// Rebuild deletedKeys and lifecycleBreaks.
	//
	// A deletion event is: entry E where OldNextKeyId != Null, pointing to entry D.
	// The minimum version among all entries pointing to D gives the deletion version.
	//
	// If D's key is NOT currently alive → add to deletedKeys.
	// If D's key IS currently alive   → the key was re-inserted; add to lifecycleBreaks.
	deletionVersionFor := make(map[uint64]types.Version)
	for _, e := range entries {
		if e.OldNextKeyId == types.NullEntryID {
			continue
		}
		pointed, ok := entryMap[e.OldNextKeyId]
		if !ok {
			continue
		}
		if pointed.Key == types.MinKey || pointed.Key == types.MaxKey {
			continue
		}
		if prev, seen := deletionVersionFor[e.OldNextKeyId]; !seen || e.Version < prev {
			deletionVersionFor[e.OldNextKeyId] = e.Version
		}
	}
	for deletedEntryID, deletionVer := range deletionVersionFor {
		del := entryMap[deletedEntryID]
		if del == nil {
			continue
		}
		rec := deletedKeyRecord{
			lastEntryID:     deletedEntryID,
			deletionVersion: deletionVer,
		}
		if _, alive := liveKeys[del.Key]; alive {
			// Key was re-inserted after deletion → store in lifecycleBreaks.
			s.lifecycleBreaks[del.Key] = append(s.lifecycleBreaks[del.Key], rec)
		} else {
			// Key is currently deleted → store in deletedKeys.
			s.deletedKeys[del.Key] = rec
		}
	}

	// Rebuild Full twigs (IDs 0 .. freshTwigID-1).
	for twigID := uint64(0); twigID < freshTwigID; twigID++ {
		leaves := rebuildTwigLeavesFromMap(twigID, entryMap)
		fd := twig.RebuildFromLeaves(leaves)

		t := &twig.Twig{
			TwigID:  twigID,
			ShardID: s.shardID,
			Status:  twig.StatusFull,
		}
		for i := range t.ActiveBits {
			t.ActiveBits[i] = 0xFF
		}
		t.ActiveCount = types.TwigSize
		t.RootHash = fd.Nodes[1]

		startID := twigID * uint64(types.TwigSize)
		for slot := 0; slot < types.TwigSize; slot++ {
			if superseded[startID+uint64(slot)] {
				t.MarkSlotInactive(slot)
			}
		}

		if t.Status == twig.StatusInactive {
			// All entries superseded — prune immediately.
			rootHash := t.RootHash
			t.TransitionToPruned()
			s.prunedTwigRoots[twigID] = rootHash
		} else {
			s.twigs[twigID] = t
		}
	}

	// Rebuild the current Fresh Twig.
	s.freshTwig = twig.NewFreshTwig(freshTwigID, s.shardID)
	startID := freshTwigID * uint64(types.TwigSize)
	for slot := 0; slot < types.TwigSize; slot++ {
		entryID := startID + uint64(slot)
		if entryID >= s.nextEntryID {
			break
		}
		e, ok := entryMap[entryID]
		if !ok {
			break
		}
		leafHash := crypto.HashEntry(
			e.Id, e.Key[:], e.Value, e.NextKey[:],
			e.OldId, e.OldNextKeyId, uint64(e.Version), e.IsDeleted,
		)
		s.freshTwig.AppendLeaf(leafHash)
	}

	return nil
}

// rebuildTwigLeavesFromMap builds the leaf-hash array for a twig purely from the
// in-memory entry map (no disk reads). Absent slots get crypto.NullHash.
func rebuildTwigLeavesFromMap(twigID uint64, entryMap map[uint64]*types.Entry) [types.TwigSize]crypto.Hash {
	var leaves [types.TwigSize]crypto.Hash
	for i := range leaves {
		leaves[i] = crypto.NullHash
	}
	startID := twigID * uint64(types.TwigSize)
	for slot := 0; slot < types.TwigSize; slot++ {
		e, ok := entryMap[startID+uint64(slot)]
		if !ok {
			continue
		}
		leaves[slot] = crypto.HashEntry(
			e.Id, e.Key[:], e.Value, e.NextKey[:],
			e.OldId, e.OldNextKeyId, uint64(e.Version), e.IsDeleted,
		)
	}
	return leaves
}
