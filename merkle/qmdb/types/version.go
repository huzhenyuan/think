package types

import "fmt"

// Version encodes both block height and transaction index into a single uint64.
// Upper 32 bits = block height, lower 32 bits = transaction index within the block.
// This guarantees strict ordering: versions from the same block are ordered by tx index,
// and versions from different blocks are ordered by block height.
type Version uint64

// NewVersion constructs a Version from block height and transaction index.
func NewVersion(blockHeight uint64, txIndex uint32) Version {
	return Version((blockHeight << 32) | uint64(txIndex))
}

// BlockHeight extracts the block height component.
func (v Version) BlockHeight() uint64 {
	return uint64(v) >> 32
}

// TxIndex extracts the transaction index component.
func (v Version) TxIndex() uint32 {
	return uint32(uint64(v) & 0xFFFFFFFF)
}

// IsZero returns true for the zero version (genesis / unset).
func (v Version) IsZero() bool {
	return v == 0
}

func (v Version) String() string {
	return fmt.Sprintf("block=%d,tx=%d", v.BlockHeight(), v.TxIndex())
}
