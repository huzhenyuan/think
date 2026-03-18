// Package crypto provides hashing primitives used throughout QMDB.
// All hashes are Keccak256 (28-byte truncation for keys, 32-byte for Merkle nodes).
package crypto

import (
	"encoding/binary"

	"golang.org/x/crypto/sha3"
)

// HashSize is the size of a Merkle node hash (32 bytes = 256 bits).
const HashSize = 32

// Hash is a 32-byte Keccak256 digest used for Merkle tree nodes.
type Hash [HashSize]byte

// NullHash is the canonical hash for an empty Twig leaf slot.
// Defined as Keccak256("QMDB_NULL_SLOT") to avoid collisions with real hashes.
var NullHash Hash

func init() {
	NullHash = Keccak256Hash([]byte("QMDB_NULL_SLOT"))
}

// Keccak256 computes the Keccak256 hash of the concatenation of all inputs.
func Keccak256(data ...[]byte) []byte {
	h := sha3.NewLegacyKeccak256()
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum(nil)
}

// Keccak256Hash computes the Keccak256 hash and returns it as a Hash.
func Keccak256Hash(data ...[]byte) Hash {
	raw := Keccak256(data...)
	var out Hash
	copy(out[:], raw)
	return out
}

// HashEntry computes the hash of a serialised Entry for use as a Twig leaf.
// We hash all meaningful fields; Value is included verbatim.
// The format must be deterministic across all nodes.
func HashEntry(
	id uint64,
	key []byte,
	value []byte,
	nextKey []byte,
	oldId uint64,
	oldNextKeyId uint64,
	version uint64,
	isDeleted bool,
) Hash {
	h := sha3.NewLegacyKeccak256()

	var buf8 [8]byte

	binary.BigEndian.PutUint64(buf8[:], id)
	h.Write(buf8[:])

	h.Write(key)
	h.Write(value)
	h.Write(nextKey)

	binary.BigEndian.PutUint64(buf8[:], oldId)
	h.Write(buf8[:])

	binary.BigEndian.PutUint64(buf8[:], oldNextKeyId)
	h.Write(buf8[:])

	binary.BigEndian.PutUint64(buf8[:], version)
	h.Write(buf8[:])

	if isDeleted {
		h.Write([]byte{1})
	} else {
		h.Write([]byte{0})
	}

	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// HashPair computes the parent hash of two sibling Merkle nodes.
// We prepend a 0x01 domain byte to distinguish internal nodes from leaf hashes.
func HashPair(left, right Hash) Hash {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte{0x01}) // domain separator: internal node
	h.Write(left[:])
	h.Write(right[:])
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// HashAppKey derives a 28-byte storage key from an application-layer key.
// Returns exactly 28 bytes (Keccak256 truncated to 28 bytes).
// The first 4 bits (nibble) determine the Shard.
func HashAppKey(appKey []byte) [28]byte {
	raw := Keccak256(appKey)
	var out [28]byte
	copy(out[:], raw[:28])
	return out
}
