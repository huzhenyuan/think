// Package eth provides Ethereum-compatible interfaces layered on top of QMDB.
//
// Two interface levels are exposed:
//
//  1. Trie — low-level key/value interface matching go-ethereum's trie.Trie.
//     Operates on raw 32-byte keys and raw byte values.
//     Extended with QMDB-specific history methods.
//
//  2. StateDB — account-level interface matching go-ethereum's vm.StateDB.
//     Manages Account objects (Balance, Nonce, CodeHash, StorageRoot).
//     Encodes accounts into the 224-byte Value field of QMDB entries.
//
// Neither interface imports go-ethereum directly to avoid the heavy dependency;
// the method signatures are deliberately compatible.
package eth

import "github.com/qmdb/types"

// Address is a 20-byte Ethereum address.
type Address [20]byte

// Hash is a 32-byte hash (for code hashes, storage roots, etc.).
type Hash [32]byte

// BigInt is a simplified big integer representation.
// In production this would be math/big.Int.
type BigInt = uint64 // simplified: balance as uint64

// ─────────────────────────────────────────────────────────────────────────────
// Trie interface (matches go-ethereum trie.Trie methods)
// ─────────────────────────────────────────────────────────────────────────────

// Trie is the low-level authenticated key/value store interface.
// Equivalent to go-ethereum's trie.Trie / state.Trie.
type Trie interface {
	// Get retrieves the value for the given key.
	// Returns nil if the key does not exist.
	Get(key []byte) ([]byte, error)

	// Update inserts or updates the value for the given key.
	Update(key, value []byte) error

	// Delete removes a key from the trie.
	Delete(key []byte) error

	// Hash returns the current root hash of the trie.
	// In QMDB this is always up-to-date (no pending-commit phase).
	Hash() Hash

	// Commit finalises the current state and returns the state root.
	// In QMDB this is a no-op (writes are committed immediately); returns current root.
	Commit() (Hash, error)

	// Prove constructs a Merkle proof for the given key and writes it into proofDB.
	// Compatible with go-ethereum's trie.Prove signature.
	Prove(key []byte, proofDB ProofWriter) error

	// ProveNonExistence constructs a non-existence (exclusion) proof for the given key.
	// Provides the predecessor entry and its NextKey to prove the key is absent.
	ProveNonExistence(key []byte, proofDB ProofWriter) error

	// ─── QMDB extensions ───────────────────────────────────────────────────

	// GetAtVersion retrieves the value of a key at a historical block/tx version.
	GetAtVersion(key []byte, version types.Version) ([]byte, error)
}

// ProofWriter is a write-only key/value store that receives proof nodes.
// Matches the ethdb.KeyValueWriter interface from go-ethereum.
type ProofWriter interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// ─────────────────────────────────────────────────────────────────────────────
// StateDB interface (matches go-ethereum's vm.StateDB)
// ─────────────────────────────────────────────────────────────────────────────

// StateDB is the account-level state database interface.
// Matches the subset of go-ethereum's vm.StateDB used by the EVM.
type StateDB interface {
	// ── Account existence ──────────────────────────────────────────────────
	CreateAccount(addr Address)
	Exist(addr Address) bool
	Empty(addr Address) bool
	DeleteAccount(addr Address)

	// ── Balance ────────────────────────────────────────────────────────────
	GetBalance(addr Address) BigInt
	AddBalance(addr Address, amount BigInt)
	SubBalance(addr Address, amount BigInt)
	SetBalance(addr Address, balance BigInt)

	// ── Nonce ──────────────────────────────────────────────────────────────
	GetNonce(addr Address) uint64
	SetNonce(addr Address, nonce uint64)

	// ── Code ───────────────────────────────────────────────────────────────
	GetCodeHash(addr Address) Hash
	GetCode(addr Address) []byte
	SetCode(addr Address, code []byte)
	GetCodeSize(addr Address) int

	// ── Storage ────────────────────────────────────────────────────────────
	GetState(addr Address, key Hash) Hash
	SetState(addr Address, key Hash, value Hash)
	GetStorageRoot(addr Address) Hash

	// ── Commit / snapshot ──────────────────────────────────────────────────

	// Prepare sets the current transaction context (hash and index).
	Prepare(txHash Hash, txIndex int)

	// IntermediateRoot computes the current state root.
	// deleteEmptyObjects: if true, delete accounts with zero balance/nonce/code.
	IntermediateRoot(deleteEmptyObjects bool) Hash

	// Finalise applies pending changes to the underlying Trie.
	Finalise(deleteEmptyObjects bool)

	// Commit writes all changes for the current block and returns the state root.
	Commit(block uint64) (Hash, error)

	// Snapshot returns a revision ID that can be used to revert to this state.
	Snapshot() int

	// RevertToSnapshot reverts all state changes since the snapshot was taken.
	RevertToSnapshot(id int)
}
