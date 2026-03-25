// Package db is the top-level coordinator of QMDB.
// It owns the 16 Shards and the UpperTree, routes operations to
// the correct Shard based on the first nibble of the hashed key,
// and exposes the block/transaction lifecycle API.
package db

import (
	"fmt"
	"sync"

	"github.com/qmdb/compaction"
	"github.com/qmdb/crypto"
	"github.com/qmdb/shard"
	"github.com/qmdb/types"
	"github.com/qmdb/upper_tree"
)

// QMDB is the main database struct.
// All public methods are goroutine-safe (each Shard has its own mutex).
type QMDB struct {
	// shards holds the 16 parallel Shards.
	shards [types.ShardCount]*shard.Shard

	// upperTree aggregates Twig roots from all shards into one global state root.
	upperTree *upper_tree.UpperTree

	// currentBlock is the block height currently being processed.
	currentBlock uint64

	// currentTx is the transaction index within the current block.
	currentTx uint32

	// dataDir is the root directory for all CSV files.
	dataDir string

	// Observer (optional) is called after every state-changing operation.
	// Used by the observability layer to dump CSV snapshots.
	Observer ObserverHook

	mu sync.Mutex // protects currentBlock/currentTx
}

// ObserverHook is a callback interface for observability.
// Implement this to receive notifications after every write.
type ObserverHook interface {
	// AfterWrite is called after each successful write operation.
	AfterWrite(db *QMDB, op string, key [types.KeySize]byte, version types.Version)
}

// Open creates and initialises a new QMDB instance.
// dataDir must exist and be writable; CSV files will be created inside it.
func Open(dataDir string) (*QMDB, error) {
	db := &QMDB{
		dataDir:   dataDir,
		upperTree: upper_tree.NewUpperTree(),
	}

	// Create all 16 Shards, injecting the root-change callback.
	var initErr error
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < types.ShardCount; i++ {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			s, err := shard.NewShard(shardID, dataDir, db.onTwigRootChange)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				initErr = fmt.Errorf("open shard %d: %w", shardID, err)
				return
			}
			db.shards[shardID] = s
		}(i)
	}
	wg.Wait()

	if initErr != nil {
		return nil, initErr
	}

	// After all shards are initialised, synchronise the UpperTree.
	// For brand-new shards insertSentinels already called onRootChange inline.
	// For recovered shards (rebuildFromLog) we replay all twig-root notifications now.
	for i := 0; i < types.ShardCount; i++ {
		db.shards[i].EmitTwigRoots()
	}

	return db, nil
}

// Close flushes and closes all Shard append logs.
func (db *QMDB) Close() error {
	for i := 0; i < types.ShardCount; i++ {
		if err := db.shards[i].Close(); err != nil {
			return fmt.Errorf("close shard %d: %w", i, err)
		}
	}
	return nil
}

// SetShardTraceHooks installs a TraceHook on every Shard.
// Call this after attaching an Observer (which provides the hook implementation).
func (db *QMDB) SetShardTraceHooks(hook shard.TraceHook) {
	for i := 0; i < types.ShardCount; i++ {
		db.shards[i].SetTraceHook(hook)
	}
}

// ──────────────────────────── Block lifecycle ─────────────────────────────────

// BeginBlock starts processing a new block.
// Must be called before any state writes for the block.
func (db *QMDB) BeginBlock(height uint64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.currentBlock = height
	db.currentTx = 0
}

// BeginTx marks the start of a new transaction within the current block.
// The transaction index determines the lower 32 bits of Version.
func (db *QMDB) BeginTx(txIndex uint32) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.currentTx = txIndex
}

// EndBlock finalises the block. Returns the state root (global Merkle root).
// The state root is always current — no extra computation needed.
// Compaction is run deterministically here so that every node triggers at the same time.
func (db *QMDB) EndBlock() crypto.Hash {
	// Run compaction for every shard. Must be called outside db.mu to avoid deadlock
	// (compaction calls Shard.Update which triggers onTwigRootChange → upperTree.UpdateTwigRoot).
	version := db.CurrentVersion()
	for i := 0; i < types.ShardCount; i++ {
		_, _ = compaction.RunCompactionIfNeeded(db.shards[i], version)
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.upperTree.StateRoot()
}

// StateRoot returns the current global state root.
func (db *QMDB) StateRoot() crypto.Hash {
	return db.upperTree.StateRoot()
}

// CurrentVersion returns the Version that will be stamped on the next write.
// It combines the current block height (set by BeginBlock) and the current
// transaction index (set by BeginTx / StateDB.Prepare).
func (db *QMDB) CurrentVersion() types.Version {
	db.mu.Lock()
	defer db.mu.Unlock()
	return types.NewVersion(db.currentBlock, db.currentTx)
}

// currentVersion is the unexported alias used internally.
func (db *QMDB) currentVersion() types.Version {
	return db.CurrentVersion()
}

// ──────────────────────────── State operations ────────────────────────────────

// Get returns the current value for the given application-layer key.
// The key is hashed to a 28-byte storage key before lookup.
func (db *QMDB) Get(appKey []byte) ([]byte, error) {
	key := crypto.HashAppKey(appKey)
	s := db.shardFor(key)
	return s.Get(key)
}

// GetByStorageKey returns the value for a raw 28-byte storage key.
func (db *QMDB) GetByStorageKey(key [types.KeySize]byte) ([]byte, error) {
	return db.shardFor(key).Get(key)
}

// Set writes a value for the given application-layer key.
// Automatically decides Insert vs Update based on key existence.
func (db *QMDB) Set(appKey []byte, value []byte) error {
	key := crypto.HashAppKey(appKey)
	return db.SetByStorageKey(key, value)
}

// SetByStorageKey performs Insert-or-Update using a raw 28-byte storage key.
// Uses Shard.Upsert which holds the write lock through both the existence check
// and the write, eliminating the TOCTOU race of a separate Get + Insert/Update.
func (db *QMDB) SetByStorageKey(key [types.KeySize]byte, value []byte) error {
	version := db.currentVersion()
	s := db.shardFor(key)
	s.SetCurrentOp("Set", version.BlockHeight(), version.TxIndex())
	if err := s.Upsert(key, value, version); err != nil {
		return err
	}
	db.notifyObserver("Set", key, version)
	return nil
}

// Delete removes a key from the state.
func (db *QMDB) Delete(appKey []byte) error {
	key := crypto.HashAppKey(appKey)
	return db.DeleteByStorageKey(key)
}

// DeleteByStorageKey removes a key given its raw 28-byte storage key.
func (db *QMDB) DeleteByStorageKey(key [types.KeySize]byte) error {
	version := db.currentVersion()
	s := db.shardFor(key)
	s.SetCurrentOp("Delete", version.BlockHeight(), version.TxIndex())
	if err := s.Delete(key, version); err != nil {
		return err
	}
	db.notifyObserver("Delete", key, version)
	return nil
}

// GetAtVersion retrieves the value of a key as it was at a specific block height + tx index.
func (db *QMDB) GetAtVersion(appKey []byte, targetVersion types.Version) ([]byte, error) {
	key := crypto.HashAppKey(appKey)
	return db.shardFor(key).GetAtVersion(key, targetVersion)
}

// ──────────────────────────── Proof operations ────────────────────────────────

// ProofForKey constructs the full Merkle proof path for the current version of a key.
// The proof allows any verifier to confirm the value is included in the state root.
func (db *QMDB) ProofForKey(appKey []byte) (*MerkleProof, error) {
	key := crypto.HashAppKey(appKey)
	return db.proofForStorageKey(key)
}

// ProofForStorageKey constructs a full Merkle proof for the raw 28-byte storage key.
// Used internally and by non-existence proofs (predecessor is identified by storage key).
func (db *QMDB) ProofForStorageKey(key [types.KeySize]byte) (*MerkleProof, error) {
	return db.proofForStorageKey(key)
}

// proofForStorageKey is the shared implementation for both proof entry points.
func (db *QMDB) proofForStorageKey(key [types.KeySize]byte) (*MerkleProof, error) {
	s := db.shardFor(key)

	// Look up the current entry (acquires+releases shard RLock).
	entry, err := s.GetEntry(key)
	if err != nil || entry == nil {
		return nil, fmt.Errorf("key not found: %x", key)
	}

	shardID := entry.ShardID()
	twigID := entry.TwigID()
	slot := int(entry.SlotIndex())

	leafHash := crypto.HashEntry(
		entry.Id, entry.Key[:], entry.Value, entry.NextKey[:],
		entry.OldId, entry.OldNextKeyId,
		uint64(entry.Version), entry.IsDeleted,
	)

	upperProof, err := db.upperTree.MerklePathForTwig(shardID, twigID)
	if err != nil {
		return nil, err
	}

	upperLeafPos, upperLeafCount := db.upperTree.LeafPosForTwig(shardID, twigID)

	// Fresh Twig: all Merkle nodes are in memory — zero disk I/O.
	freshTwig := s.FreshTwigSnapshot()
	if freshTwig.TwigID == twigID {
		twigProof := freshTwig.MerkleProof(slot)
		return &MerkleProof{
			EntryID:            entry.Id,
			Key:                key,
			Value:              entry.Value,
			NextKey:            entry.NextKey,
			OldId:              entry.OldId,
			OldNextKeyId:       entry.OldNextKeyId,
			Version:            entry.Version,
			TwigID:             twigID,
			SlotIndex:          slot,
			ShardID:            shardID,
			LeafHash:           leafHash,
			TwigProof:          twigProof,
			UpperTreeProof:     upperProof,
			UpperTreeLeafPos:   upperLeafPos,
			UpperTreeLeafCount: upperLeafCount,
			StateRoot:          db.upperTree.StateRoot(),
			IsPartial:          false,
		}, nil
	}

	// Full Twig: rebuild internal Merkle tree from CSV (1 sequential read per slot).
	twigProof, err := s.BuildProofForFullTwig(twigID, slot)
	if err != nil {
		return nil, fmt.Errorf("rebuild full twig proof: %w", err)
	}

	return &MerkleProof{
		EntryID:            entry.Id,
		Key:                key,
		Value:              entry.Value,
		NextKey:            entry.NextKey,
		OldId:              entry.OldId,
		OldNextKeyId:       entry.OldNextKeyId,
		Version:            entry.Version,
		TwigID:             twigID,
		SlotIndex:          slot,
		ShardID:            shardID,
		LeafHash:           leafHash,
		TwigProof:          twigProof,
		UpperTreeProof:     upperProof,
		UpperTreeLeafPos:   upperLeafPos,
		UpperTreeLeafCount: upperLeafCount,
		StateRoot:          db.upperTree.StateRoot(),
		IsPartial:          false,
	}, nil
}

// FindPredecessorEntry returns the entry for the largest active key strictly less
// than HashAppKey(appKey). Used to build non-existence proofs.
func (db *QMDB) FindPredecessorEntry(appKey []byte) (*types.Entry, error) {
	key := crypto.HashAppKey(appKey)
	return db.shardFor(key).FindPredecessorEntry(key)
}

// ──────────────────────────── Inspection helpers ──────────────────────────────

// ShardFor returns the Shard for a given application-layer key (for testing/observation).
func (db *QMDB) ShardForKey(appKey []byte) *shard.Shard {
	key := crypto.HashAppKey(appKey)
	return db.shardFor(key)
}

// Shard returns the Shard by ID (0..15).
func (db *QMDB) Shard(id int) *shard.Shard {
	return db.shards[id]
}

// UpperTree returns the upper Merkle tree.
func (db *QMDB) UpperTree() *upper_tree.UpperTree {
	return db.upperTree
}

// DataDir returns the data directory.
func (db *QMDB) DataDir() string {
	return db.dataDir
}

// ──────────────────────────── internal helpers ────────────────────────────────

// shardFor returns the Shard responsible for the given 28-byte storage key.
// The first nibble (4 bits) of the key determines the Shard (0..15).
func (db *QMDB) shardFor(key [types.KeySize]byte) *shard.Shard {
	shardID := int(key[0] >> 4)
	return db.shards[shardID]
}

// onTwigRootChange is the RootChangeCallback registered with every Shard.
// Called under the Shard's write lock; do not re-acquire it here.
func (db *QMDB) onTwigRootChange(shardID int, twigID uint64, newTwigRoot crypto.Hash) {
	db.upperTree.UpdateTwigRoot(shardID, twigID, newTwigRoot)
}

func (db *QMDB) notifyObserver(op string, key [types.KeySize]byte, version types.Version) {
	if db.Observer != nil {
		db.Observer.AfterWrite(db, op, key, version)
	}
}

// ──────────────────────────── MerkleProof ─────────────────────────────────────

// MerkleProof holds a complete proof that an Entry is included in the global state root.
type MerkleProof struct {
	EntryID      uint64
	Key          [types.KeySize]byte
	Value        []byte
	NextKey      [types.KeySize]byte
	OldId        uint64
	OldNextKeyId uint64
	Version      types.Version
	TwigID       uint64
	SlotIndex    int
	ShardID      int
	LeafHash     crypto.Hash

	// TwigProof: TwigMerkleDepth (11) sibling hashes from leaf to Twig root.
	TwigProof []crypto.Hash

	// UpperTreeProof: sibling hashes from Twig root to global state root.
	UpperTreeProof []crypto.Hash

	// UpperTreeLeafPos is the 0-based leaf position of this Twig in the upper tree.
	// UpperTreeLeafCount is the total leaf count (ShardCount * maxTwigsPerShard) at proof time.
	// Both are needed by Verify() to reproduce the heap-index walk correctly.
	UpperTreeLeafPos   int
	UpperTreeLeafCount int

	StateRoot crypto.Hash
	IsPartial bool // always false in this implementation (Full Twig proofs are now supported)
}

// Verify checks the proof against the provided state root.
// Returns true if the proof walks correctly from the entry's leaf hash to expectedRoot.
func (p *MerkleProof) Verify(expectedRoot crypto.Hash) bool {
	if p.IsPartial {
		return false
	}

	// Step 1: recompute the leaf hash from the full entry data in the proof.
	recomputed := crypto.HashEntry(
		p.EntryID, p.Key[:], p.Value, p.NextKey[:],
		p.OldId, p.OldNextKeyId,
		uint64(p.Version), false,
	)
	if recomputed != p.LeafHash {
		return false
	}

	current := p.LeafHash

	// Step 2: walk the twig-internal proof (leaf → twig root).
	// Heap index of the leaf: TwigSize + slot. Since TwigSize = 2048 (even),
	// parity at each level is determined solely by the slot value divided by 2^level.
	slot := p.SlotIndex
	for _, sibling := range p.TwigProof {
		if slot%2 == 0 {
			current = crypto.HashPair(current, sibling)
		} else {
			current = crypto.HashPair(sibling, current)
		}
		slot /= 2
	}

	// Step 3: walk the upper-tree proof (twig root → global state root).
	// Heap index of the twig leaf: leafCount + leafPos.  leafCount is always
	// ShardCount * maxTwigsPerShard = 16 * 2^k, which is always even at every
	// level we traverse, so parity is determined solely by leafPos / 2^level.
	leafPos := p.UpperTreeLeafPos
	for _, sibling := range p.UpperTreeProof {
		if leafPos%2 == 0 {
			current = crypto.HashPair(current, sibling)
		} else {
			current = crypto.HashPair(sibling, current)
		}
		leafPos /= 2
	}

	return current == expectedRoot
}
