package types

import "fmt"

const (
	// KeySize is the fixed size of hashed keys (Keccak256 truncated to 28 bytes).
	// The first 4 bits (nibble) determine the Shard (0-15), giving 16 shards.
	// The first 9 bytes are used as the B-tree index key prefix.
	KeySize = 28

	// ValueMaxSize is the maximum Value payload stored in an Entry (224 bytes).
	// The StateDB layer is responsible for encoding account state into this space.
	ValueMaxSize = 224

	// ShardCount is the number of parallel shards. Fixed at 16 (first nibble of hashed key).
	ShardCount = 16

	// TwigSize is the number of Entry slots in one Twig. 2048 = 2^11.
	// The internal Merkle tree has 11 levels (depth 0..10, root at level 11).
	TwigSize = 2048

	// TwigMerkleDepth is log2(TwigSize).
	TwigMerkleDepth = 11

	// IndexPrefixSize is the number of bytes of the hashed key used as B-tree index key.
	// 9 bytes ≈ 72 bits of entropy → collision probability ≈ 1/2^72, practically negligible.
	IndexPrefixSize = 9
)

// NullEntryID is the sentinel value for "no previous entry" in OldId / OldNextKeyId chains.
const NullEntryID uint64 = 0xFFFFFFFFFFFFFFFF

// MinKey is the sentinel key for the MIN node (start of ordered linked list).
var MinKey [KeySize]byte // all zeros: 0x00...00

// MaxKey is the sentinel key for the MAX node (end of ordered linked list).
var MaxKey [KeySize]byte // all 0xFF

func init() {
	for i := range MaxKey {
		MaxKey[i] = 0xFF
	}
}

// Entry is the fundamental storage unit of QMDB. Every state write appends a new
// Entry; old entries are never modified in-place. This makes all writes sequential.
//
// Layout of an Entry in the append log (CSV columns map 1:1 to these fields):
//
//	Id           — global monotonically increasing ID. Encodes Twig + slot:
//	                 TwigID  = Id / TwigSize
//	                 SlotIdx = Id % TwigSize
//	Key          — 28-byte Keccak256 hash of the application-layer key.
//	                 First nibble → Shard; first 9 bytes → B-tree index.
//	Value        — raw state bytes (≤224 B). Empty slice means deleted.
//	NextKey      — Key of the adjacent successor in the ordered linked list.
//	                 All active entries form a sorted chain: MIN→…→MAX.
//	                 This chain enables non-existence proofs.
//	OldId        — Entry ID of the previous version of *this* Key, or NullEntryID.
//	                 Following the chain gives the full history of one account.
//	OldNextKeyId — Entry ID of the entry that last changed the NextKey relationship,
//	                 or NullEntryID. Required to reconstruct non-existence proofs
//	                 at arbitrary historical heights.
//	Version      — block height (upper 32 bits) | tx index (lower 32 bits).
//	                 Enables point-in-time queries at transaction granularity.
//	IsDeleted    — true when this Entry records a deletion (Value is empty/ignored).
type Entry struct {
	// Id is the global, monotonically-increasing entry identifier.
	// TwigID = Id/TwigSize, SlotIndex = Id%TwigSize.
	Id uint64

	// Key is the 28-byte hash of the application-layer key.
	// First 4 bits decide the Shard; first 9 bytes are used as B-tree index key prefix.
	Key [KeySize]byte

	// Value holds the encoded state payload (up to 224 bytes).
	// Empty when IsDeleted is true.
	Value []byte

	// NextKey is the Key of the immediately-following active entry in hash-sorted order.
	// Together, all active entries form a linked list: MIN → … → MAX.
	NextKey [KeySize]byte

	// OldId links to the previous version of this Key (same entry, different time).
	// NullEntryID means this Key has no prior history.
	OldId uint64

	// OldNextKeyId links to the previous entry that defined this NextKey relationship.
	// Required for reconstructing non-existence proofs at historical block heights.
	OldNextKeyId uint64

	// Version captures the write context: upper 32 bits = block height, lower 32 bits = tx index.
	Version Version

	// IsDeleted marks a tombstone entry produced by a Delete operation.
	IsDeleted bool
}

// TwigID returns which Twig this entry belongs to.
func (e *Entry) TwigID() uint64 {
	return e.Id / TwigSize
}

// SlotIndex returns the position of this entry within its Twig (0..TwigSize-1).
func (e *Entry) SlotIndex() uint64 {
	return e.Id % TwigSize
}

// ShardID returns the Shard this entry belongs to (first nibble of Key).
func (e *Entry) ShardID() int {
	return int(e.Key[0] >> 4)
}

// IndexPrefix returns the 9-byte B-tree index key for this entry.
func (e *Entry) IndexPrefix() [IndexPrefixSize]byte {
	var prefix [IndexPrefixSize]byte
	copy(prefix[:], e.Key[:IndexPrefixSize])
	return prefix
}

// String returns a human-readable representation for debugging.
func (e *Entry) String() string {
	return fmt.Sprintf("Entry{Id=%d, Key=%x, NextKey=%x, Version=%s, Deleted=%v}",
		e.Id, e.Key, e.NextKey, e.Version, e.IsDeleted)
}
