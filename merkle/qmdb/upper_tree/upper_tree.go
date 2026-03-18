// Package upper_tree implements the upper-level Merkle tree that aggregates
// Twig root hashes from all 16 Shards into a single global state root.
//
// Tree structure
// ══════════════
//
// Node layout (1-indexed binary heap, "segment tree" style):
//
//	nodes[1]                           = global state root
//	nodes[2], nodes[3]                 = root's two children
//	...
//	nodes[leafCount .. 2*leafCount-1]  = leaf slots (one per Twig position)
//
// Leaf position for a Twig:
//
//	pos = shardID * maxTwigsPerShard + twigID
//
// Tree relations:  parent(i) = i>>1,  children = 2i,  2i+1.
//
// Dynamic sizing
// ══════════════
// maxTwigsPerShard starts at 64 and doubles whenever a new TwigID would
// overflow the current capacity.  leafCount = ShardCount * maxTwigsPerShard
// (always a power of 2).  On resize all twig roots are replayed from the
// twigRoots snapshot map and the tree is rebuilt bottom-up.
//
// Thread safety
// ═════════════
// All exported methods are goroutine-safe via a single RWMutex.
// UpdateTwigRoot acquires a write lock; read-only methods acquire a read lock.
package upper_tree

import (
	"fmt"
	"sync"

	"github.com/qmdb/crypto"
	"github.com/qmdb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

// NodeSnapshot is one node returned by AllNodes for CSV export.
type NodeSnapshot struct {
	// Index is the 1-based heap index.
	Index int
	// LeftChild is the heap index of the left child (0 = none / leaf).
	LeftChild int
	// RightChild is the heap index of the right child (0 = none / leaf).
	RightChild int
	// Hash is the 32-byte hash stored at this node.
	Hash crypto.Hash
}

// ─────────────────────────────────────────────────────────────────────────────
// UpperTree
// ─────────────────────────────────────────────────────────────────────────────

// twigKey uniquely identifies one Twig across all Shards.
type twigKey struct {
	shardID int
	twigID  uint64
}

// UpperTree maintains the global state root by aggregating all Twig root hashes.
// It is kept entirely in memory; its size grows proportionally to the number
// of Twigs (O(totalTwigs * 32 bytes)).
type UpperTree struct {
	mu sync.RWMutex

	// nodes is the flat 1-indexed binary heap.
	// nodes[1] = global state root; index 0 is unused.
	// Length = 2*leafCount + 1.
	nodes []crypto.Hash

	// leafCount is the number of leaf slots.
	// Equals ShardCount * maxTwigsPerShard; always a power of 2.
	leafCount int

	// maxTwigsPerShard is the capacity per Shard.
	// Doubles when a TwigID would exceed it.
	maxTwigsPerShard uint64

	// twigRoots is the authoritative snapshot of every known Twig root hash.
	// Used to repopulate the tree when grow() is called.
	twigRoots map[twigKey]crypto.Hash
}

// NewUpperTree creates an empty upper Merkle tree.
// Initial capacity: 64 Twigs per Shard → 1 024 leaf slots total.
func NewUpperTree() *UpperTree {
	const initialMaxTwigsPerShard = 64

	ut := &UpperTree{
		maxTwigsPerShard: initialMaxTwigsPerShard,
		twigRoots:        make(map[twigKey]crypto.Hash),
	}

	ut.leafCount = int(ut.maxTwigsPerShard) * types.ShardCount

	// Index 0 unused; valid indices are 1 .. 2*leafCount.
	ut.nodes = make([]crypto.Hash, 2*ut.leafCount+1)
	for i := range ut.nodes {
		ut.nodes[i] = crypto.NullHash
	}

	// Build the all-NullHash tree so nodes[1] has a deterministic value.
	ut.rebuild()
	return ut
}

// ─────────────────────────────────────────────────────────────────────────────
// Write methods
// ─────────────────────────────────────────────────────────────────────────────

// UpdateTwigRoot records a new root hash for the given (shardID, twigID) and
// propagates the change up the tree to the global state root.
//
// Called after every Entry append in a Shard (via the RootChangeCallback).
// Complexity: O(log(leafCount)) — entirely in memory, zero disk I/O.
func (ut *UpperTree) UpdateTwigRoot(shardID int, twigID uint64, newRoot crypto.Hash) {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	// Grow the tree if this TwigID exceeds per-Shard capacity.
	for twigID >= ut.maxTwigsPerShard {
		ut.grow()
	}

	// Store in snapshot map so we can replay after future grow calls.
	ut.twigRoots[twigKey{shardID, twigID}] = newRoot

	// Write the new hash into the leaf slot and update ancestors.
	leafPos := ut.leafPosition(shardID, twigID)
	heapIdx := ut.leafCount + leafPos
	ut.nodes[heapIdx] = newRoot
	ut.recomputePath(heapIdx)
}

// ─────────────────────────────────────────────────────────────────────────────
// Read methods
// ─────────────────────────────────────────────────────────────────────────────

// StateRoot returns the current global state root (nodes[1]).
// Always up-to-date; no "commit" step is needed.
func (ut *UpperTree) StateRoot() crypto.Hash {
	ut.mu.RLock()
	defer ut.mu.RUnlock()
	return ut.nodes[1]
}

// GetTwigRoot returns the cached root hash for a specific Twig.
// Returns NullHash if the Twig has not been recorded yet.
func (ut *UpperTree) GetTwigRoot(shardID int, twigID uint64) crypto.Hash {
	ut.mu.RLock()
	defer ut.mu.RUnlock()
	if h, ok := ut.twigRoots[twigKey{shardID, twigID}]; ok {
		return h
	}
	return crypto.NullHash
}

// MerklePathForTwig returns the sibling hashes that prove a Twig's root hash
// is included in the global state root.
//
// The slice is ordered leaf→root (length = log2(leafCount)).
// Used to combine with a Twig-internal proof to build a complete Merkle proof.
func (ut *UpperTree) MerklePathForTwig(shardID int, twigID uint64) ([]crypto.Hash, error) {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	if twigID >= ut.maxTwigsPerShard {
		return nil, fmt.Errorf(
			"twigID %d for shard %d exceeds current max %d",
			twigID, shardID, ut.maxTwigsPerShard-1,
		)
	}

	leafPos := ut.leafPosition(shardID, twigID)
	if leafPos >= ut.leafCount {
		return nil, fmt.Errorf("leafPos %d >= leafCount %d", leafPos, ut.leafCount)
	}

	// Walk from the leaf up to the root, collecting sibling hashes.
	var path []crypto.Hash
	idx := ut.leafCount + leafPos
	for idx > 1 {
		sibling := idx ^ 1 // flip lowest bit → sibling index
		path = append(path, ut.nodes[sibling])
		idx >>= 1 // move to parent
	}
	return path, nil
}

// AllNodes returns a snapshot of all non-NullHash nodes for CSV export.
// The slice is ordered by heap index (root first, leaves last).
func (ut *UpperTree) AllNodes() []NodeSnapshot {
	ut.mu.RLock()
	defer ut.mu.RUnlock()

	var result []NodeSnapshot
	for i := 1; i < len(ut.nodes); i++ {
		if ut.nodes[i] == crypto.NullHash {
			continue // skip empty slots for a compact CSV
		}
		left := 2 * i
		right := 2*i + 1
		// For leaf nodes both children are out-of-range; report as 0.
		if left >= len(ut.nodes) {
			left = 0
		}
		if right >= len(ut.nodes) {
			right = 0
		}
		result = append(result, NodeSnapshot{
			Index:      i,
			LeftChild:  left,
			RightChild: right,
			Hash:       ut.nodes[i],
		})
	}
	return result
}

// NodeCount returns the total number of allocated node slots (including NullHash nodes).
func (ut *UpperTree) NodeCount() int {
	ut.mu.RLock()
	defer ut.mu.RUnlock()
	return len(ut.nodes)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers (all callers must hold mu appropriately)
// ─────────────────────────────────────────────────────────────────────────────

// leafPosition converts (shardID, twigID) to a 0-based leaf slot index.
func (ut *UpperTree) leafPosition(shardID int, twigID uint64) int {
	return int(uint64(shardID)*ut.maxTwigsPerShard + twigID)
}

// recomputePath recomputes all ancestor hashes on the path from heapIdx to nodes[1].
// Caller must hold write lock.
func (ut *UpperTree) recomputePath(heapIdx int) {
	idx := heapIdx >> 1 // start at the parent of the changed leaf
	for idx >= 1 {
		ut.nodes[idx] = crypto.HashPair(ut.nodes[2*idx], ut.nodes[2*idx+1])
		idx >>= 1
	}
}

// rebuild performs a full bottom-up pass to compute every internal node from
// the current leaf values.  Used at initialisation and after grow().
// Caller must hold write lock.
func (ut *UpperTree) rebuild() {
	for i := ut.leafCount - 1; i >= 1; i-- {
		ut.nodes[i] = crypto.HashPair(ut.nodes[2*i], ut.nodes[2*i+1])
	}
}

// grow doubles maxTwigsPerShard to accommodate a larger TwigID.
//
//  1. Double maxTwigsPerShard and recompute leafCount.
//  2. Allocate a new node array filled with NullHash.
//  3. Re-place every known Twig root at its new leaf position.
//  4. Rebuild all internal nodes bottom-up.
//
// Caller must hold write lock.
func (ut *UpperTree) grow() {
	ut.maxTwigsPerShard *= 2
	ut.leafCount = int(ut.maxTwigsPerShard) * types.ShardCount

	// Fresh array — all NullHash.
	newNodes := make([]crypto.Hash, 2*ut.leafCount+1)
	for i := range newNodes {
		newNodes[i] = crypto.NullHash
	}
	ut.nodes = newNodes

	// Replay known twig roots at their new positions (positions shift because
	// maxTwigsPerShard changed).
	for key, hash := range ut.twigRoots {
		leafPos := ut.leafPosition(key.shardID, key.twigID)
		ut.nodes[ut.leafCount+leafPos] = hash
	}

	// Recompute all internal nodes.
	ut.rebuild()
}
