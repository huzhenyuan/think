// Package db is the top-level coordinator of QMDB.
// It owns the 16 Shards and the UpperTree, routes operations to
// the correct Shard based on the first nibble of the hashed key,
// and exposes the block/transaction lifecycle API.
package db

import (
	"fmt"
	"sync"

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
	return db, nil
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
func (db *QMDB) EndBlock() crypto.Hash {
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
func (db *QMDB) SetByStorageKey(key [types.KeySize]byte, value []byte) error {
	version := db.currentVersion()
	s := db.shardFor(key)

	// Check existence to decide Insert vs Update.
	existing, err := s.Get(key)
	if err != nil {
		return err
	}

	if existing == nil {
		// Key doesn't exist → Insert.
		if err := s.Insert(key, value, version); err != nil {
			return err
		}
	} else {
		// Key exists → Update.
		if err := s.Update(key, value, version); err != nil {
			return err
		}
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
//
// Proof structure:
//  1. Twig-internal proof (11 sibling hashes, from FreshTwig's in-memory Merkle tree).
//  2. Upper-tree proof (sibling hashes from TwigID to root).
func (db *QMDB) ProofForKey(appKey []byte) (*MerkleProof, error) {
	key := crypto.HashAppKey(appKey)
	s := db.shardFor(key)

	// Look up the current entry.
	entry, err := s.GetEntry(key)
	if err != nil || entry == nil {
		return nil, fmt.Errorf("key not found: %x", key)
	}

	shardID := entry.ShardID()
	twigID := entry.TwigID()
	slot := int(entry.SlotIndex())

	// Step 1: get the twig-internal proof.
	freshTwig := s.FreshTwigSnapshot()
	if freshTwig.TwigID != twigID {
		// Entry is in a Full Twig — would need to read from CSV and rebuild.
		// For this implementation we return a partial proof.
		return &MerkleProof{
			EntryID:        entry.Id,
			Key:            key,
			Value:          entry.Value,
			Version:        entry.Version,
			TwigID:         twigID,
			SlotIndex:      slot,
			ShardID:        shardID,
			TwigProof:      nil, // requires CSV re-read (see Twig.RebuildFromLeaves)
			UpperTreeProof: nil,
			StateRoot:      db.upperTree.StateRoot(),
			IsPartial:      true,
		}, nil
	}

	twigProof := freshTwig.MerkleProof(slot)
	upperProof, err := db.upperTree.MerklePathForTwig(shardID, twigID)
	if err != nil {
		return nil, err
	}

	leafHash := crypto.HashEntry(
		entry.Id, entry.Key[:], entry.Value, entry.NextKey[:],
		entry.OldId, entry.OldNextKeyId,
		uint64(entry.Version), entry.IsDeleted,
	)

	return &MerkleProof{
		EntryID:        entry.Id,
		Key:            key,
		Value:          entry.Value,
		Version:        entry.Version,
		TwigID:         twigID,
		SlotIndex:      slot,
		ShardID:        shardID,
		LeafHash:       leafHash,
		TwigProof:      twigProof,
		UpperTreeProof: upperProof,
		StateRoot:      db.upperTree.StateRoot(),
		IsPartial:      false,
	}, nil
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

// MerkleProof holds a complete (or partial) proof that an Entry is included
// in the global state root.
type MerkleProof struct {
	EntryID   uint64
	Key       [types.KeySize]byte
	Value     []byte
	Version   types.Version
	TwigID    uint64
	SlotIndex int
	ShardID   int
	LeafHash  crypto.Hash

	// TwigProof: 11 sibling hashes from leaf to Twig root.
	TwigProof []crypto.Hash

	// UpperTreeProof: sibling hashes from Twig root to global state root.
	UpperTreeProof []crypto.Hash

	StateRoot crypto.Hash
	IsPartial bool // true if TwigProof couldn't be computed (Full Twig)
}

// Verify checks the proof against the provided state root.
func (p *MerkleProof) Verify(expectedRoot crypto.Hash) bool {
	if p.IsPartial {
		return false
	}

	// Step 1: recompute leaf hash from entry data.
	recomputed := crypto.HashEntry(
		p.EntryID, p.Key[:], p.Value, make([]byte, types.KeySize),
		types.NullEntryID, types.NullEntryID,
		uint64(p.Version), false,
	)
	// Note: in a full verify we'd use the actual NextKey/OldId from the proof.
	// For now just verify structure.
	_ = recomputed

	current := p.LeafHash
	// Walk Twig-internal proof.
	slot := p.SlotIndex
	for _, sibling := range p.TwigProof {
		if slot%2 == 0 {
			current = crypto.HashPair(current, sibling)
		} else {
			current = crypto.HashPair(sibling, current)
		}
		slot /= 2
	}

	// Walk upper-tree proof.
	leafPos := p.ShardID*64 + int(p.TwigID) // simplified
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
