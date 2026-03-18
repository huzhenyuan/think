// Package twig implements the two-level Merkle structure used by QMDB:
//   - Each Twig holds TwigSize (2048) Entry leaf hashes in an 11-level binary Merkle tree.
//   - Once a Twig is full its internal nodes are discarded; only the 32-byte root hash survives.
//   - The 2048 roots from all Twigs feed into the upper-level Merkle tree (see upper_tree package).
//
// Twig state machine:
//
//	Fresh  → all Entry + Merkle nodes live in RAM (~640 KB per Twig).
//	Full   → Entry data flushed to CSV; only RootHash[32B] + ActiveBits[256B] remain in RAM.
//	Inactive → all ActiveBits are 0 (no live entries); still 288 B in RAM.
//	Pruned → removed from the upper tree; zero RAM footprint.
package twig

import (
	"github.com/qmdb/crypto"
	"github.com/qmdb/types"
)

// Status enumerates the four lifecycle stages of a Twig.
type Status int

const (
	// StatusFresh: Twig is being actively written. Full entry data + Merkle nodes in RAM.
	StatusFresh Status = iota
	// StatusFull: Twig is full (all TwigSize slots written). Entry data on CSV.
	// Only RootHash + ActiveBits kept in RAM (288 bytes).
	StatusFull
	// StatusInactive: All entries in this Twig have been superseded. Still 288 B in RAM
	// because the root hash is still referenced by the upper Merkle tree.
	StatusInactive
	// StatusPruned: The upper tree subtree containing this Twig has been collapsed.
	// This Twig consumes zero RAM.
	StatusPruned
)

func (s Status) String() string {
	switch s {
	case StatusFresh:
		return "Fresh"
	case StatusFull:
		return "Full"
	case StatusInactive:
		return "Inactive"
	case StatusPruned:
		return "Pruned"
	default:
		return "Unknown"
	}
}

const (
	// activeBitsBytes is the byte size of the ActiveBits bitmap: TwigSize / 8 = 256 bytes.
	activeBitsBytes = types.TwigSize / 8
)

// FreshData holds all in-memory data for a Twig in StatusFresh.
// Once the Twig transitions to Full, this is set to nil and GC'd.
//
// Internal Merkle tree layout (1-indexed, standard binary heap numbering):
//
//	Level 0  (leaf):  nodes[TwigSize .. 2*TwigSize-1]   → 2048 leaves
//	Level 1:          nodes[TwigSize/2 .. TwigSize-1]
//	...
//	Level 11 (root):  nodes[1]
//
// We use 1-indexed heap layout so parent(i) = i/2, children = 2i, 2i+1.
// Index 0 is unused.
type FreshData struct {
	// Nodes stores all Merkle tree nodes in heap order.
	// Size = 2 * TwigSize (indices 0..2*TwigSize-1; index 0 unused).
	Nodes [2 * types.TwigSize]crypto.Hash

	// NextSlot is the index of the next free leaf slot (0..TwigSize-1).
	// When NextSlot == TwigSize, the Twig is full.
	NextSlot int
}

// Twig represents one unit of the two-level Merkle structure.
type Twig struct {
	// TwigID is the globally-unique Twig identifier: TwigID = EntryId / TwigSize.
	TwigID uint64

	// ShardID is the Shard this Twig belongs to (0..15).
	ShardID int

	// Status is the current lifecycle stage.
	Status Status

	// RootHash is the 32-byte Merkle root of this Twig's 2048 leaf slots.
	// Valid in all states: in Fresh it is kept up-to-date as entries are added.
	RootHash crypto.Hash

	// ActiveBits is a 256-byte bitmap (one bit per slot). Bit i = 1 means Entry i
	// is still the current (latest) version of its Key. Set to 0 when a newer
	// Entry for the same Key is written, or on Delete.
	// Only meaningful in Full and Inactive; not used in Fresh (all slots active by definition
	// until entries are superseded later, which is tracked once Full).
	ActiveBits [activeBitsBytes]byte

	// ActiveCount is the number of bits set in ActiveBits. Used to detect Inactive state
	// and to decide Compaction (ActiveCount / TwigSize < 0.5).
	ActiveCount int

	// Fresh holds the full Merkle tree data. nil when Status != StatusFresh.
	Fresh *FreshData
}

// NewFreshTwig creates an empty Fresh Twig with all leaf slots set to NullHash.
func NewFreshTwig(twigID uint64, shardID int) *Twig {
	t := &Twig{
		TwigID:  twigID,
		ShardID: shardID,
		Status:  StatusFresh,
		Fresh:   &FreshData{NextSlot: 0},
	}
	// Initialise all leaf slots to NullHash.
	for i := types.TwigSize; i < 2*types.TwigSize; i++ {
		t.Fresh.Nodes[i] = crypto.NullHash
	}
	// Build initial internal nodes (all leaf slots are NullHash → root is also deterministic).
	t.rebuildFromLeaves()
	return t
}

// IsFull returns true when all TwigSize slots have been written.
func (t *Twig) IsFull() bool {
	return t.Status == StatusFresh && t.Fresh.NextSlot == types.TwigSize
}

// NextFreeSlot returns the index of the next empty leaf slot. Panics if the Twig is not Fresh.
func (t *Twig) NextFreeSlot() int {
	if t.Status != StatusFresh {
		panic("NextFreeSlot called on non-Fresh Twig")
	}
	return t.Fresh.NextSlot
}

// AppendLeaf writes a leaf hash into the next available slot, recomputes the Merkle path
// from that leaf up to the root, and returns the updated root hash.
//
// Panics if called on a non-Fresh or full Twig.
func (t *Twig) AppendLeaf(leafHash crypto.Hash) crypto.Hash {
	if t.Status != StatusFresh {
		panic("AppendLeaf called on non-Fresh Twig")
	}
	if t.Fresh.NextSlot >= types.TwigSize {
		panic("Twig is full; cannot append more leaves")
	}

	slot := t.Fresh.NextSlot
	t.Fresh.NextSlot++

	// Set the leaf in heap layout: leaf at index (TwigSize + slot).
	t.Fresh.Nodes[types.TwigSize+slot] = leafHash

	// Recompute path from leaf to root.
	t.recomputePathFromSlot(slot)

	t.RootHash = t.Fresh.Nodes[1]
	return t.RootHash
}

// GetLeafHash returns the leaf hash at the given slot. Only valid for Fresh Twigs.
func (t *Twig) GetLeafHash(slot int) crypto.Hash {
	if t.Status != StatusFresh {
		panic("GetLeafHash called on non-Fresh Twig")
	}
	return t.Fresh.Nodes[types.TwigSize+slot]
}

// MerkleProof returns the sibling hashes needed to prove that the leaf at `slot`
// hashes up to this Twig's root. The proof is ordered from leaf level to root level
// (length = TwigMerkleDepth = 11 hashes).
//
// For a Full Twig this requires rebuilding the tree from disk (caller's responsibility).
// This method only works on a Fresh Twig.
func (t *Twig) MerkleProof(slot int) []crypto.Hash {
	if t.Status != StatusFresh {
		panic("MerkleProof on non-Fresh Twig: caller must rebuild from disk first")
	}
	proof := make([]crypto.Hash, 0, types.TwigMerkleDepth)
	idx := types.TwigSize + slot
	for idx > 1 {
		sibling := idx ^ 1 // flip least-significant bit to get sibling
		proof = append(proof, t.Fresh.Nodes[sibling])
		idx >>= 1
	}
	return proof
}

// TransitionToFull transitions a Fresh Twig to Full status.
// The internal FreshData is released, freeing ~640 KB.
// All TwigSize entries are assumed active when transitioning, so ActiveCount = TwigSize.
// The caller is responsible for flushing entry data to CSV before calling this.
func (t *Twig) TransitionToFull() {
	if t.Status != StatusFresh {
		panic("TransitionToFull called on non-Fresh Twig")
	}
	// All slots are initially active.
	for i := range t.ActiveBits {
		t.ActiveBits[i] = 0xFF
	}
	t.ActiveCount = types.TwigSize

	// Release the large FreshData; keep only RootHash (already set by last AppendLeaf).
	t.Fresh = nil
	t.Status = StatusFull
}

// MarkSlotInactive clears the ActiveBit for the given slot.
// Returns true if this caused the Twig to become Inactive (all bits now 0).
func (t *Twig) MarkSlotInactive(slot int) bool {
	if t.Status != StatusFull && t.Status != StatusInactive {
		return false
	}
	byteIdx := slot / 8
	bitMask := byte(1 << (7 - (slot % 8)))
	if t.ActiveBits[byteIdx]&bitMask != 0 {
		t.ActiveBits[byteIdx] &^= bitMask
		t.ActiveCount--
	}
	if t.ActiveCount == 0 {
		t.Status = StatusInactive
		return true
	}
	return false
}

// IsSlotActive returns true if the entry at the given slot is still the current version.
func (t *Twig) IsSlotActive(slot int) bool {
	if t.Status == StatusFresh {
		return slot < t.Fresh.NextSlot
	}
	byteIdx := slot / 8
	bitMask := byte(1 << (7 - (slot % 8)))
	return t.ActiveBits[byteIdx]&bitMask != 0
}

// TransitionToPruned marks the Twig as Pruned. Called when the upper Merkle tree
// collapses the subtree containing this Twig.
func (t *Twig) TransitionToPruned() {
	t.ActiveBits = [activeBitsBytes]byte{}
	t.Status = StatusPruned
}

// NeedsCompaction returns true when less than half the entries are still active.
// This is the deterministic trigger used by the Compaction worker.
// Only applicable to Full Twigs.
func (t *Twig) NeedsCompaction() bool {
	if t.Status != StatusFull {
		return false
	}
	return t.ActiveCount*2 < types.TwigSize
}

// RebuildFromLeaves rebuilds the Twig's internal Merkle tree from a slice of
// TwigSize leaf hashes. Used when generating a proof for a Full Twig
// (after reading all entries back from CSV).
// Returns the rebuilt FreshData; does NOT permanently change t.Status.
func RebuildFromLeaves(leaves [types.TwigSize]crypto.Hash) *FreshData {
	fd := &FreshData{NextSlot: types.TwigSize}
	for i := 0; i < types.TwigSize; i++ {
		fd.Nodes[types.TwigSize+i] = leaves[i]
	}
	// Bottom-up pass.
	for i := types.TwigSize - 1; i >= 1; i-- {
		fd.Nodes[i] = crypto.HashPair(fd.Nodes[2*i], fd.Nodes[2*i+1])
	}
	return fd
}

// ──────────────────────────── internal helpers ────────────────────────────────

// rebuildFromLeaves builds the entire Merkle tree in a single bottom-up pass.
// Used during initialisation (all leaves = NullHash).
func (t *Twig) rebuildFromLeaves() {
	for i := types.TwigSize - 1; i >= 1; i-- {
		t.Fresh.Nodes[i] = crypto.HashPair(t.Fresh.Nodes[2*i], t.Fresh.Nodes[2*i+1])
	}
	t.RootHash = t.Fresh.Nodes[1]
}

// recomputePathFromSlot recomputes the internal nodes on the path from leaf `slot`
// up to the root. Only the log2(TwigSize) = 11 nodes on this path change.
func (t *Twig) recomputePathFromSlot(slot int) {
	idx := (types.TwigSize + slot) >> 1 // parent of the leaf
	for idx >= 1 {
		t.Fresh.Nodes[idx] = crypto.HashPair(t.Fresh.Nodes[2*idx], t.Fresh.Nodes[2*idx+1])
		idx >>= 1
	}
}
