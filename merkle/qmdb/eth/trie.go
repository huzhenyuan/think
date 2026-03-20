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
//
// Proof structure: find the predecessor P such that P.Key < key ≤ P.NextKey,
// then provide the full Merkle proof for P. Any verifier can confirm:
//  1. P is genuinely in the state (Merkle proof checks out against state root).
//  2. P.NextKey > key, so there is no entry between P and P.NextKey.
//
// The following fields are written to proofDB:
//
//	non_existence_key — the queried key (raw bytes)
//	pred_key          — predecessor's storage key (28 bytes)
//	pred_next_key     — predecessor's NextKey (28 bytes), proves the gap
//	pred_leaf_hash    — leaf hash of the predecessor entry
//	pred_twig_proof_N — sibling hashes of the twig-internal proof
//	pred_upper_proof_N — sibling hashes of the upper-tree proof
//	state_root        — state root at proof time
func (t *QMDBTrie) ProveNonExistence(key []byte, proofDB ProofWriter) error {
	// 1. Confirm non-existence.
	val, err := t.Get(key)
	if err != nil {
		return err
	}
	if val != nil {
		return fmt.Errorf("ProveNonExistence: key %x actually exists", key)
	}

	// 2. Find P: the predecessor entry (P.Key < key ≤ P.NextKey).
	predEntry, err := t.qmdb.FindPredecessorEntry(key)
	if err != nil {
		return fmt.Errorf("ProveNonExistence: find predecessor: %w", err)
	}

	// 3. Generate the full Merkle proof for P.
	predProof, err := t.qmdb.ProofForStorageKey(predEntry.Key)
	if err != nil {
		return fmt.Errorf("ProveNonExistence: proof for predecessor: %w", err)
	}

	// 4. Write all proof components into proofDB.
	if err := proofDB.Put([]byte("non_existence_key"), key); err != nil {
		return err
	}
	if err := proofDB.Put([]byte("pred_key"), predEntry.Key[:]); err != nil {
		return err
	}
	if err := proofDB.Put([]byte("pred_next_key"), predEntry.NextKey[:]); err != nil {
		return err
	}
	if err := proofDB.Put([]byte("pred_leaf_hash"), predProof.LeafHash[:]); err != nil {
		return err
	}
	for i, h := range predProof.TwigProof {
		nodeKey := fmt.Sprintf("pred_twig_proof_%d", i)
		if err := proofDB.Put([]byte(nodeKey), h[:]); err != nil {
			return err
		}
	}
	for i, h := range predProof.UpperTreeProof {
		nodeKey := fmt.Sprintf("pred_upper_proof_%d", i)
		if err := proofDB.Put([]byte(nodeKey), h[:]); err != nil {
			return err
		}
	}
	if err := proofDB.Put([]byte("state_root"), predProof.StateRoot[:]); err != nil {
		return err
	}
	return nil
}

// GetAtVersion retrieves the historical value at the given Version.
func (t *QMDBTrie) GetAtVersion(key []byte, version types.Version) ([]byte, error) {
	return t.qmdb.GetAtVersion(key, version)
}
