// QMDBStateDB implements the eth.StateDB interface.
//
// Architecture:
//   - Accounts are stored in QMDB as key = Keccak28(address), value = 224-byte encoded Account.
//   - Contract storage is stored as key = Keccak28(address ++ slot), value = 32-byte hash.
//   - Code is stored as key = Keccak28("code" ++ codeHash), value = bytecode.
//   - An in-memory dirty cache accumulates writes within a transaction; they are
//     flushed to QMDB on Finalise() / Commit().
//   - Snapshot/Revert are implemented with a journal (undo log) over the in-memory cache.
package eth

import (
	"encoding/binary"
	"fmt"

	"github.com/qmdb/crypto"
	"github.com/qmdb/db"
	"github.com/qmdb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// In-memory pending state
// ─────────────────────────────────────────────────────────────────────────────

// pendingAccount is an account that has been modified but not yet flushed to QMDB.
type pendingAccount struct {
	account   Account
	deleted   bool
	codeSet   bool
	code      []byte
	storageKV map[Hash]Hash // pending storage writes
}

// journalEntry is one undo record for a snapshot/revert.
type journalEntry struct {
	addr        Address
	prevAccount *pendingAccount // nil = did not exist before
}

// ─────────────────────────────────────────────────────────────────────────────
// QMDBStateDB
// ─────────────────────────────────────────────────────────────────────────────

// QMDBStateDB implements eth.StateDB using QMDB as the backing store.
type QMDBStateDB struct {
	qmdb *db.QMDB

	// pending holds accounts modified in the current transaction, not yet flushed.
	pending map[Address]*pendingAccount

	// snapshots is the journal stack; each snapshot captures the dirty state at the
	// time Snapshot() was called so RevertToSnapshot() can undo changes.
	snapshots [][]journalEntry

	// current journal (flushed to snapshots on Snapshot()).
	journal []journalEntry

	// currentTxHash is the hash of the currently-executing transaction.
	currentTxHash Hash

	// currentTxIndex is the index of the currently-executing transaction.
	currentTxIndex int

	// currentBlock is the block height being processed.
	currentBlock uint64
}

// NewQMDBStateDB creates a new StateDB backed by the given QMDB instance.
func NewQMDBStateDB(qmdb *db.QMDB) *QMDBStateDB {
	return &QMDBStateDB{
		qmdb:    qmdb,
		pending: make(map[Address]*pendingAccount),
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Account existence
// ─────────────────────────────────────────────────────────────────────────────

func (s *QMDBStateDB) CreateAccount(addr Address) {
	if !s.Exist(addr) {
		s.journalBefore(addr) // record that addr did not exist
		s.pending[addr] = &pendingAccount{
			storageKV: make(map[Hash]Hash),
		}
	}
}

func (s *QMDBStateDB) Exist(addr Address) bool {
	if p, ok := s.pending[addr]; ok {
		return !p.deleted
	}
	val, _ := s.qmdb.GetByStorageKey(addrToStorageKey(addr))
	return val != nil
}

func (s *QMDBStateDB) Empty(addr Address) bool {
	acc := s.getAccount(addr)
	if acc == nil {
		return true
	}
	return isEmptyAccount(acc)
}

func (s *QMDBStateDB) DeleteAccount(addr Address) {
	s.journalBefore(addr)
	if p, ok := s.pending[addr]; ok {
		p.deleted = true
		return
	}
	s.pending[addr] = &pendingAccount{deleted: true, storageKV: make(map[Hash]Hash)}
}

// ─────────────────────────────────────────────────────────────────────────────
// Balance
// ─────────────────────────────────────────────────────────────────────────────

func (s *QMDBStateDB) GetBalance(addr Address) BigInt {
	acc := s.getAccount(addr)
	if acc == nil {
		return 0
	}
	return acc.Balance
}

func (s *QMDBStateDB) AddBalance(addr Address, amount BigInt) {
	acc := s.getOrCreatePending(addr)
	s.journalBefore(addr)
	acc.account.Balance += amount
}

func (s *QMDBStateDB) SubBalance(addr Address, amount BigInt) {
	acc := s.getOrCreatePending(addr)
	s.journalBefore(addr)
	if acc.account.Balance < amount {
		acc.account.Balance = 0
	} else {
		acc.account.Balance -= amount
	}
}

func (s *QMDBStateDB) SetBalance(addr Address, balance BigInt) {
	acc := s.getOrCreatePending(addr)
	s.journalBefore(addr)
	acc.account.Balance = balance
}

// ─────────────────────────────────────────────────────────────────────────────
// Nonce
// ─────────────────────────────────────────────────────────────────────────────

func (s *QMDBStateDB) GetNonce(addr Address) uint64 {
	acc := s.getAccount(addr)
	if acc == nil {
		return 0
	}
	return acc.Nonce
}

func (s *QMDBStateDB) SetNonce(addr Address, nonce uint64) {
	acc := s.getOrCreatePending(addr)
	s.journalBefore(addr)
	acc.account.Nonce = nonce
}

// ─────────────────────────────────────────────────────────────────────────────
// Code
// ─────────────────────────────────────────────────────────────────────────────

func (s *QMDBStateDB) GetCodeHash(addr Address) Hash {
	acc := s.getAccount(addr)
	if acc == nil {
		return Hash{}
	}
	return acc.CodeHash
}

func (s *QMDBStateDB) GetCode(addr Address) []byte {
	acc := s.getAccount(addr)
	if acc == nil {
		return nil
	}
	var zeroHash Hash
	if acc.CodeHash == zeroHash {
		return nil
	}
	// Check pending code.
	if p, ok := s.pending[addr]; ok && p.codeSet {
		return p.code
	}
	// Load from QMDB.
	codeKey := codeStorageKey(acc.CodeHash)
	code, _ := s.qmdb.GetByStorageKey(codeKey)
	return code
}

func (s *QMDBStateDB) SetCode(addr Address, code []byte) {
	acc := s.getOrCreatePending(addr)
	s.journalBefore(addr)
	// Compute code hash.
	rawHash := crypto.Keccak256(code)
	var codeHash Hash
	copy(codeHash[:], rawHash[:32])
	acc.account.CodeHash = codeHash
	acc.codeSet = true
	acc.code = append([]byte{}, code...)
}

func (s *QMDBStateDB) GetCodeSize(addr Address) int {
	return len(s.GetCode(addr))
}

// ─────────────────────────────────────────────────────────────────────────────
// Storage
// ─────────────────────────────────────────────────────────────────────────────

func (s *QMDBStateDB) GetState(addr Address, slot Hash) Hash {
	// Check pending.
	if p, ok := s.pending[addr]; ok {
		if v, ok2 := p.storageKV[slot]; ok2 {
			return v
		}
	}
	// Load from QMDB.
	storageKey := storageSlotKey(addr, slot)
	val, _ := s.qmdb.GetByStorageKey(storageKey)
	if len(val) != 32 {
		return Hash{}
	}
	var h Hash
	copy(h[:], val)
	return h
}

func (s *QMDBStateDB) SetState(addr Address, slot Hash, value Hash) {
	p := s.getOrCreatePending(addr)
	s.journalBefore(addr)
	p.storageKV[slot] = value
}

func (s *QMDBStateDB) GetStorageRoot(addr Address) Hash {
	acc := s.getAccount(addr)
	if acc == nil {
		return Hash{}
	}
	return acc.StorageRoot
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction context
// ─────────────────────────────────────────────────────────────────────────────

func (s *QMDBStateDB) Prepare(txHash Hash, txIndex int) {
	s.currentTxHash = txHash
	s.currentTxIndex = txIndex
	s.qmdb.BeginTx(uint32(txIndex))
}

// ─────────────────────────────────────────────────────────────────────────────
// Finalise / Commit
// ─────────────────────────────────────────────────────────────────────────────

// Finalise flushes all pending changes to the underlying QMDB.
// This is called at the end of each transaction (or block, depending on EVM version).
func (s *QMDBStateDB) Finalise(deleteEmptyObjects bool) {
	for addr, p := range s.pending {
		if p.deleted || (deleteEmptyObjects && isEmptyAccount(&p.account)) {
			_ = s.qmdb.DeleteByStorageKey(addrToStorageKey(addr))
			continue
		}

		// Flush account via SetByStorageKey so the version is taken from QMDB's
		// current block/tx context (set by BeginBlock + BeginTx / Prepare).
		key := addrToStorageKey(addr)
		encoded := encodeAccount(&p.account)
		_ = s.qmdb.SetByStorageKey(key, encoded)

		// Flush code if set.
		if p.codeSet {
			codeKey := codeStorageKey(p.account.CodeHash)
			_ = s.qmdb.SetByStorageKey(codeKey, p.code)
		}

		// Flush contract storage slots.
		for slot, value := range p.storageKV {
			storageKey := storageSlotKey(addr, slot)
			var zero Hash
			if value == zero {
				_ = s.qmdb.DeleteByStorageKey(storageKey)
			} else {
				_ = s.qmdb.SetByStorageKey(storageKey, value[:])
			}
		}
	}

	// Clear pending state.
	s.pending = make(map[Address]*pendingAccount)
	s.journal = nil
}

// IntermediateRoot computes the current state root after applying pending changes.
func (s *QMDBStateDB) IntermediateRoot(deleteEmptyObjects bool) Hash {
	s.Finalise(deleteEmptyObjects)
	root := s.qmdb.StateRoot()
	var h Hash
	copy(h[:], root[:])
	return h
}

// Commit finalises the block and returns the state root.
func (s *QMDBStateDB) Commit(block uint64) (Hash, error) {
	s.currentBlock = block
	s.qmdb.BeginBlock(block)
	root := s.IntermediateRoot(true)
	_ = s.qmdb.EndBlock()
	return root, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot / Revert
// ─────────────────────────────────────────────────────────────────────────────

// Snapshot captures the current pending state and returns a revision ID.
func (s *QMDBStateDB) Snapshot() int {
	id := len(s.snapshots)
	// Store a deep copy of the current journal.
	journalCopy := make([]journalEntry, len(s.journal))
	copy(journalCopy, s.journal)
	s.snapshots = append(s.snapshots, journalCopy)
	s.journal = nil
	return id
}

// RevertToSnapshot undoes all changes since the snapshot with the given ID.
func (s *QMDBStateDB) RevertToSnapshot(id int) {
	if id >= len(s.snapshots) {
		panic(fmt.Sprintf("RevertToSnapshot: invalid snapshot id %d (max=%d)", id, len(s.snapshots)-1))
	}

	// Undo all journal entries from the most recent snapshot down to id.
	// Process in reverse order.
	for i := len(s.snapshots) - 1; i > id; i-- {
		for j := len(s.snapshots[i]) - 1; j >= 0; j-- {
			entry := s.snapshots[i][j]
			if entry.prevAccount == nil {
				delete(s.pending, entry.addr)
			} else {
				s.pending[entry.addr] = entry.prevAccount
			}
		}
	}
	// Undo current journal.
	for j := len(s.journal) - 1; j >= 0; j-- {
		entry := s.journal[j]
		if entry.prevAccount == nil {
			delete(s.pending, entry.addr)
		} else {
			s.pending[entry.addr] = entry.prevAccount
		}
	}

	s.snapshots = s.snapshots[:id+1]
	s.journal = nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

// getAccount returns the latest account for addr (pending takes priority over QMDB).
func (s *QMDBStateDB) getAccount(addr Address) *Account {
	if p, ok := s.pending[addr]; ok {
		if p.deleted {
			return nil
		}
		return &p.account
	}
	val, _ := s.qmdb.GetByStorageKey(addrToStorageKey(addr))
	if val == nil {
		return nil
	}
	acc, _ := decodeAccount(val)
	return acc
}

// getOrCreatePending returns the pending entry for addr, creating one if needed.
func (s *QMDBStateDB) getOrCreatePending(addr Address) *pendingAccount {
	if p, ok := s.pending[addr]; ok {
		return p
	}
	// Load existing account to initialise pending.
	p := &pendingAccount{storageKV: make(map[Hash]Hash)}
	if acc := s.getAccount(addr); acc != nil {
		p.account = *acc
	}
	s.pending[addr] = p
	return p
}

// journalBefore records the pre-change state of addr for potential revert.
// Only the first modification within a snapshot period is recorded.
func (s *QMDBStateDB) journalBefore(addr Address) {
	// Check if already journaled in this snapshot period.
	for _, j := range s.journal {
		if j.addr == addr {
			return
		}
	}
	var prev *pendingAccount
	if p, ok := s.pending[addr]; ok {
		// Deep copy.
		cp := *p
		slotCopy := make(map[Hash]Hash, len(p.storageKV))
		for k, v := range p.storageKV {
			slotCopy[k] = v
		}
		cp.storageKV = slotCopy
		prev = &cp
	}
	s.journal = append(s.journal, journalEntry{addr: addr, prevAccount: prev})
}

// ─────────────────────────────────────────────────────────────────────────────
// Key derivation
// ─────────────────────────────────────────────────────────────────────────────

// addrToStorageKey derives the 28-byte QMDB storage key for an account address.
// The domain prefix "acct:" ensures no collision with code or storage slot keys.
func addrToStorageKey(addr Address) [types.KeySize]byte {
	return crypto.HashAppKey(append([]byte("acct:"), addr[:]...))
}

// storageSlotKey derives the 28-byte QMDB storage key for a contract storage slot.
func storageSlotKey(addr Address, slot Hash) [types.KeySize]byte {
	buf := make([]byte, 0, 5+20+32)
	buf = append(buf, []byte("slot:")...)
	buf = append(buf, addr[:]...)
	buf = append(buf, slot[:]...)
	return crypto.HashAppKey(buf)
}

// codeStorageKey derives the 28-byte QMDB storage key for contract bytecode.
func codeStorageKey(codeHash Hash) [types.KeySize]byte {
	buf := make([]byte, 0, 5+32)
	buf = append(buf, []byte("code:")...)
	buf = append(buf, codeHash[:]...)
	return crypto.HashAppKey(buf)
}

// uint64ToBytes converts a uint64 to an 8-byte big-endian slice.
func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
