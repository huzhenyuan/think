// Package shard implements the per-Shard B-tree in-memory index.
//
// Index key: first 9 bytes of the hashed 28-byte storage key.
// Index value: the Entry ID (uint64) of the most recent version of that key.
//
// Collision handling: if two keys share the same 9-byte prefix, both are stored.
// On lookup the caller reads the Entry from the CSV log and checks the full 28-byte Key.
package shard

import (
	"github.com/google/btree"
	"github.com/qmdb/types"
)

// IndexEntry is one record in the B-tree index.
// Sorted first by KeyPrefix, then by EntryID (newest first within the same prefix).
type IndexEntry struct {
	// KeyPrefix is the first 9 bytes of the 28-byte hashed key.
	// Used as the sort key in the B-tree.
	KeyPrefix [types.IndexPrefixSize]byte

	// FullKey is the full 28-byte key. Used to resolve prefix collisions.
	FullKey [types.KeySize]byte

	// EntryID is the ID of the latest Entry for this key.
	EntryID uint64
}

// less defines the ordering for the B-tree.
// Primary sort: KeyPrefix (lexicographic). Ties broken by FullKey then EntryID.
func less(a, b IndexEntry) bool {
	for i := 0; i < types.IndexPrefixSize; i++ {
		if a.KeyPrefix[i] != b.KeyPrefix[i] {
			return a.KeyPrefix[i] < b.KeyPrefix[i]
		}
	}
	// Same prefix: break tie on full key.
	for i := 0; i < types.KeySize; i++ {
		if a.FullKey[i] != b.FullKey[i] {
			return a.FullKey[i] < b.FullKey[i]
		}
	}
	return false
}

// BTreeIndex wraps a google/btree for IndexEntry with typed accessors.
type BTreeIndex struct {
	tree *btree.BTreeG[IndexEntry]
}

// NewBTreeIndex creates a new empty B-tree index.
// degree = 32 gives good cache performance for billions of entries.
func NewBTreeIndex() *BTreeIndex {
	return &BTreeIndex{
		tree: btree.NewG[IndexEntry](32, less),
	}
}

// Upsert inserts or replaces the index entry for the given full key and entry ID.
func (idx *BTreeIndex) Upsert(fullKey [types.KeySize]byte, entryID uint64) {
	var prefix [types.IndexPrefixSize]byte
	copy(prefix[:], fullKey[:types.IndexPrefixSize])
	idx.tree.ReplaceOrInsert(IndexEntry{
		KeyPrefix: prefix,
		FullKey:   fullKey,
		EntryID:   entryID,
	})
}

// Delete removes the index entry for the given full key.
// Returns true if an entry was removed.
func (idx *BTreeIndex) Delete(fullKey [types.KeySize]byte) bool {
	var prefix [types.IndexPrefixSize]byte
	copy(prefix[:], fullKey[:types.IndexPrefixSize])
	_, removed := idx.tree.Delete(IndexEntry{
		KeyPrefix: prefix,
		FullKey:   fullKey,
	})
	return removed
}

// Lookup returns the Entry ID for the given full key, and whether it was found.
func (idx *BTreeIndex) Lookup(fullKey [types.KeySize]byte) (entryID uint64, found bool) {
	var prefix [types.IndexPrefixSize]byte
	copy(prefix[:], fullKey[:types.IndexPrefixSize])
	item, ok := idx.tree.Get(IndexEntry{
		KeyPrefix: prefix,
		FullKey:   fullKey,
	})
	if !ok {
		return 0, false
	}
	return item.EntryID, true
}

// FindPredecessor returns the largest key strictly less than the given key,
// along with its Entry ID. Used to find the predecessor when inserting a new key
// into the ordered linked list (NextKey chain).
// Returns (zeroKey, 0, false) if no predecessor exists.
func (idx *BTreeIndex) FindPredecessor(fullKey [types.KeySize]byte) (predKey [types.KeySize]byte, predID uint64, found bool) {
	var prefix [types.IndexPrefixSize]byte
	copy(prefix[:], fullKey[:types.IndexPrefixSize])
	pivot := IndexEntry{KeyPrefix: prefix, FullKey: fullKey}

	idx.tree.DescendLessOrEqual(pivot, func(item IndexEntry) bool {
		// Skip the key itself (equal).
		if item.FullKey == fullKey {
			return true
		}
		predKey = item.FullKey
		predID = item.EntryID
		found = true
		return false // stop after first match
	})
	return predKey, predID, found
}

// Len returns the number of entries in the index.
func (idx *BTreeIndex) Len() int {
	return idx.tree.Len()
}

// Ascend iterates over all index entries in ascending key order.
func (idx *BTreeIndex) Ascend(fn func(IndexEntry) bool) {
	idx.tree.Ascend(fn)
}
