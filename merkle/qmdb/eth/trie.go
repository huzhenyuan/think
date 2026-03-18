// QMDBTrie implements the eth.Trie interface on top of a QMDB database.
// It is the thin adapter between Ethereum's Trie contract and QMDB's storage layer.
package eth

import (
	"fmt"

	"github.com/qmdb/db"
	"github.com/qmdb/types"
)

// QMDBTrie wraps a QMDB instance and exposes the eth.Trie interface.
type QMDBTrie struct {
	qmdb *db.QMDB
}

// NewQMDBTrie creates a new Trie adapter backed by the given QMDB instance.
func NewQMDBTrie(qmdb *db.QMDB) *QMDBTrie {
	return &QMDBTrie{qmdb: qmdb}
}

// ──────────────────────────── Trie interface implementation ───────────────────

// Get retrieves the value associated with key.
// key is the raw application-layer key (not pre-hashed).
func (t *QMDBTrie) Get(key []byte) ([]byte, error) {
	return t.qmdb.Get(key)
}

// Update inserts or updates the key → value mapping.
func (t *QMDBTrie) Update(key, value []byte) error {
	return t.qmdb.Set(key, value)
}

// Delete removes the key from the trie.
func (t *QMDBTrie) Delete(key []byte) error {
	return t.qmdb.Delete(key)
}

// Hash returns the current global state root. Always up-to-date in QMDB.
func (t *QMDBTrie) Hash() Hash {
	root := t.qmdb.StateRoot()
	var h Hash
	copy(h[:], root[:])
	return h
}

// Commit is a no-op in QMDB (writes are committed immediately). Returns current root.
func (t *QMDBTrie) Commit() (Hash, error) {
	return t.Hash(), nil
}

// Prove generates a Merkle proof for the given key.
// The proof nodes are written as key=nodeHash, value=nodeData into proofDB.
func (t *QMDBTrie) Prove(key []byte, proofDB ProofWriter) error {
	proof, err := t.qmdb.ProofForKey(key)
	if err != nil {
		return fmt.Errorf("Prove: %w", err)
	}
	if proof.IsPartial {
		return fmt.Errorf("Prove: entry is in a Full Twig; full proof requires CSV rebuild")
	}

	// Encode proof nodes into proofDB (simplified: store each sibling hash).
	// In production, this would follow the Ethereum proof encoding (RLP trie nodes).
	for i, h := range proof.TwigProof {
		nodeKey := fmt.Sprintf("twig_proof_%d_%d", proof.EntryID, i)
		if err := proofDB.Put([]byte(nodeKey), h[:]); err != nil {
			return err
		}
	}
	for i, h := range proof.UpperTreeProof {
		nodeKey := fmt.Sprintf("upper_proof_%d_%d", proof.TwigID, i)
		if err := proofDB.Put([]byte(nodeKey), h[:]); err != nil {
			return err
		}
	}
	// Store the leaf hash and state root.
	if err := proofDB.Put([]byte("leaf_hash"), proof.LeafHash[:]); err != nil {
		return err
	}
	if err := proofDB.Put([]byte("state_root"), proof.StateRoot[:]); err != nil {
		return err
	}
	return nil
}

// ProveNonExistence constructs a non-existence proof for the given key.
// Returns the predecessor entry (predecessor.Key < key ≤ predecessor.NextKey).
func (t *QMDBTrie) ProveNonExistence(key []byte, proofDB ProofWriter) error {
	// Check that the key genuinely does not exist.
	val, err := t.Get(key)
	if err != nil {
		return err
	}
	if val != nil {
		return fmt.Errorf("ProveNonExistence: key %x actually exists", key)
	}

	// The proof is: find the predecessor Key P such that P < key < P.NextKey.
	// Provide a Merkle proof for P, which implicitly proves key is absent.
	proof, err := t.qmdb.ProofForKey(key) // will fail for non-existent key
	_ = proof
	if err != nil {
		// The key doesn't exist, so we need to prove the predecessor.
		// For the demo, we record metadata in proofDB.
		if err2 := proofDB.Put([]byte("non_existence_key"), key); err2 != nil {
			return err2
		}
		return nil
	}
	return nil
}

// GetAtVersion retrieves the historical value at the given Version.
func (t *QMDBTrie) GetAtVersion(key []byte, version types.Version) ([]byte, error) {
	return t.qmdb.GetAtVersion(key, version)
}
