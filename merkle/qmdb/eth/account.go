package eth

import (
	"encoding/binary"
	"fmt"
)

// Account holds the state of one Ethereum account.
// This is the canonical in-memory representation; it is encoded to/from
// a compact binary form when stored in QMDB's 224-byte Value field.
//
// Encoding layout (fixed-size, little-endian):
//
//	Offset  Size  Field
//	  0       8   Balance (uint64, wei — simplified from *big.Int)
//	  8       8   Nonce (uint64)
//	 16      32   CodeHash ([32]byte, Keccak256 of bytecode; zero = no code)
//	 48      32   StorageRoot ([32]byte, Merkle root of storage trie; zero = empty)
//	 80     144   Reserved / future use (zero-padded to reach 224 bytes)
//
// Total: 224 bytes (fits exactly in types.ValueMaxSize).
type Account struct {
	Balance     uint64
	Nonce       uint64
	CodeHash    [32]byte // zero = no code (EOA)
	StorageRoot [32]byte // zero = empty storage
}

const accountEncodedSize = 224

// encodeAccount serialises an Account into a fixed 224-byte slice.
func encodeAccount(a *Account) []byte {
	buf := make([]byte, accountEncodedSize)
	binary.LittleEndian.PutUint64(buf[0:8], a.Balance)
	binary.LittleEndian.PutUint64(buf[8:16], a.Nonce)
	copy(buf[16:48], a.CodeHash[:])
	copy(buf[48:80], a.StorageRoot[:])
	// bytes 80..223 remain zero (reserved)
	return buf
}

// decodeAccount deserialises an Account from a 224-byte slice.
func decodeAccount(data []byte) (*Account, error) {
	if len(data) < accountEncodedSize {
		return nil, fmt.Errorf("decodeAccount: expected %d bytes, got %d", accountEncodedSize, len(data))
	}
	a := &Account{}
	a.Balance = binary.LittleEndian.Uint64(data[0:8])
	a.Nonce = binary.LittleEndian.Uint64(data[8:16])
	copy(a.CodeHash[:], data[16:48])
	copy(a.StorageRoot[:], data[48:80])
	return a, nil
}

// isEmptyAccount returns true if the account has zero balance, zero nonce, and no code.
func isEmptyAccount(a *Account) bool {
	var zeroHash [32]byte
	return a.Balance == 0 && a.Nonce == 0 && a.CodeHash == zeroHash
}
