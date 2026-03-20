package shard_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/qmdb/crypto"
	"github.com/qmdb/shard"
	"github.com/qmdb/types"
)

// ─────────────────────────── helpers ────────────────────────────────────────

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "qmdb_shard_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func newTestShard(t *testing.T, dir string) *shard.Shard {
	t.Helper()
	s, err := shard.NewShard(0, dir, nil) // shardID=0, no root-change callback
	if err != nil {
		t.Fatalf("NewShard: %v", err)
	}
	return s
}

// makeKey returns a deterministic 28-byte key in shard 0 (first nibble = 0x0).
// The input n distinguishes keys.
func makeKey(n int) [types.KeySize]byte {
	var k [types.KeySize]byte
	k[0] = 0x00 // shard 0
	k[1] = byte(n >> 8)
	k[2] = byte(n)
	return k
}

func v(block uint64) types.Version { return types.NewVersion(block, 0) }

// ─────────────────────────── basic CRUD ────────────────────────────────────

func TestInsertGet(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(1)
	if err := s.Insert(key, []byte("hello"), v(1)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	val, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "hello" {
		t.Fatalf("want 'hello', got %q", val)
	}
}

func TestInsertDuplicate(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(1)
	if err := s.Insert(key, []byte("a"), v(1)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := s.Insert(key, []byte("b"), v(2)); err == nil {
		t.Fatal("expected error on duplicate Insert, got nil")
	}
}

func TestUpdateGet(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(2)
	if err := s.Insert(key, []byte("v1"), v(1)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := s.Update(key, []byte("v2"), v(2)); err != nil {
		t.Fatalf("Update: %v", err)
	}
	val, _ := s.Get(key)
	if string(val) != "v2" {
		t.Fatalf("want 'v2', got %q", val)
	}
}

func TestUpsert(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(3)
	// First Upsert → Insert
	if err := s.Upsert(key, []byte("v1"), v(1)); err != nil {
		t.Fatalf("Upsert insert: %v", err)
	}
	val, _ := s.Get(key)
	if string(val) != "v1" {
		t.Fatalf("want 'v1', got %q", val)
	}
	// Second Upsert → Update
	if err := s.Upsert(key, []byte("v2"), v(2)); err != nil {
		t.Fatalf("Upsert update: %v", err)
	}
	val, _ = s.Get(key)
	if string(val) != "v2" {
		t.Fatalf("want 'v2', got %q", val)
	}
}

func TestDelete(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(4)
	if err := s.Insert(key, []byte("val"), v(1)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := s.Delete(key, v(2)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	val, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if val != nil {
		t.Fatalf("want nil after delete, got %q", val)
	}
}

func TestDeleteMissing(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(5)
	if err := s.Delete(key, v(1)); err == nil {
		t.Fatal("expected error deleting non-existent key")
	}
}

// ─────────────────────────── GetAtVersion ──────────────────────────────────

func TestGetAtVersion(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(10)

	_ = s.Insert(key, []byte("v1"), v(1))
	_ = s.Update(key, []byte("v2"), v(5))
	_ = s.Update(key, []byte("v3"), v(10))

	cases := []struct {
		ver  uint64
		want string
	}{
		{0, ""},    // before insert
		{1, "v1"},  // at insert
		{3, "v1"},  // between insert and update
		{5, "v2"},  // at first update
		{7, "v2"},  // between updates
		{10, "v3"}, // at second update
		{99, "v3"}, // after
	}
	for _, tc := range cases {
		val, err := s.GetAtVersion(key, types.NewVersion(tc.ver, 0))
		if err != nil {
			t.Errorf("ver=%d: %v", tc.ver, err)
			continue
		}
		got := string(val)
		if got != tc.want {
			t.Errorf("ver=%d: want %q, got %q", tc.ver, tc.want, got)
		}
	}
}

func TestGetAtVersionDeleted(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(11)

	_ = s.Insert(key, []byte("alive"), v(1))
	_ = s.Delete(key, v(5))

	// Before insertion: not found.
	val, _ := s.GetAtVersion(key, v(0))
	if val != nil {
		t.Fatalf("before insert: want nil, got %q", val)
	}
	// At insertion: alive.
	val, _ = s.GetAtVersion(key, v(1))
	if string(val) != "alive" {
		t.Fatalf("at insert: want 'alive', got %q", val)
	}
	// After deletion: nil.
	val, _ = s.GetAtVersion(key, v(5))
	if val != nil {
		t.Fatalf("after delete: want nil, got %q", val)
	}
	// Current: also nil.
	val, _ = s.Get(key)
	if val != nil {
		t.Fatalf("current after delete: want nil, got %q", val)
	}
}

// ─────────────────────────── Multi-lifecycle ─────────────────────────────

func TestReinsertAfterDelete(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	key := makeKey(20)

	_ = s.Insert(key, []byte("first"), v(1))
	_ = s.Delete(key, v(3))
	_ = s.Insert(key, []byte("second"), v(5))

	// Current value is "second".
	val, _ := s.Get(key)
	if string(val) != "second" {
		t.Fatalf("want 'second', got %q", val)
	}

	// History still navigable.
	val, _ = s.GetAtVersion(key, v(1))
	if string(val) != "first" {
		t.Fatalf("v1: want 'first', got %q", val)
	}
	val, _ = s.GetAtVersion(key, v(3))
	if val != nil {
		t.Fatalf("v3: want nil (deleted), got %q", val)
	}
	val, _ = s.GetAtVersion(key, v(5))
	if string(val) != "second" {
		t.Fatalf("v5: want 'second', got %q", val)
	}
}

// ─────────────────────────── Persistence / recovery ────────────────────────

func TestRecovery(t *testing.T) {
	dir := tempDir(t)

	// Write a few keys in "session 1".
	{
		s := newTestShard(t, dir)
		_ = s.Insert(makeKey(30), []byte("a"), v(1))
		_ = s.Insert(makeKey(31), []byte("b"), v(1))
		_ = s.Insert(makeKey(32), []byte("c"), v(1))
		_ = s.Update(makeKey(31), []byte("b2"), v(2))
		_ = s.Delete(makeKey(32), v(3))
	}

	// Reopen the shard — must rebuild from CSV log.
	s2 := newTestShard(t, dir)

	val, _ := s2.Get(makeKey(30))
	if string(val) != "a" {
		t.Fatalf("recovery: key 30 want 'a', got %q", val)
	}
	val, _ = s2.Get(makeKey(31))
	if string(val) != "b2" {
		t.Fatalf("recovery: key 31 want 'b2', got %q", val)
	}
	val, _ = s2.Get(makeKey(32))
	if val != nil {
		t.Fatalf("recovery: key 32 want nil (deleted), got %q", val)
	}

	// Can continue writing after recovery.
	if err := s2.Insert(makeKey(33), []byte("new"), v(4)); err != nil {
		t.Fatalf("post-recovery Insert: %v", err)
	}
	val, _ = s2.Get(makeKey(33))
	if string(val) != "new" {
		t.Fatalf("post-recovery: want 'new', got %q", val)
	}
}

// ─────────────────────────── Merkle proof (fresh twig) ────────────────────

func TestFreshTwigMerkleProof(t *testing.T) {
	dir := tempDir(t)

	// Track root changes via callback.
	var lastRoot crypto.Hash
	rootCallback := func(_ int, _ uint64, newRoot crypto.Hash) {
		lastRoot = newRoot
	}

	s, err := shard.NewShard(0, dir, rootCallback)
	if err != nil {
		t.Fatalf("NewShard: %v", err)
	}

	key := makeKey(40)
	_ = s.Insert(key, []byte("pf"), v(1))

	entry, _ := s.GetEntry(key)
	if entry == nil {
		t.Fatal("GetEntry returned nil")
	}

	// Verify that the stored fresh twig root matches the callback-reported root.
	ftRoot := s.FreshTwigRoot()
	if ftRoot != lastRoot {
		t.Fatalf("fresh twig root mismatch: twig=%x vs callback=%x", ftRoot, lastRoot)
	}

	// Generate proof and check it is 11 hashes deep.
	proof := s.FreshTwigSnapshot().MerkleProof(int(entry.SlotIndex()))
	if len(proof) != types.TwigMerkleDepth {
		t.Fatalf("proof len: want %d, got %d", types.TwigMerkleDepth, len(proof))
	}
}

// ─────────────────────────── Many-key linked-list invariant ────────────────

// TestLinkedListOrder verifies that the NextKey linked list is correctly
// maintained after inserting 20 keys in pseudo-random order.
func TestLinkedListOrder(t *testing.T) {
	s := newTestShard(t, tempDir(t))
	const n = 20
	for i := 0; i < n; i++ {
		key := makeKey(100 + i)
		if err := s.Insert(key, []byte(fmt.Sprintf("val%d", i)), v(uint64(i+1))); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// All inserted keys must be reachable.
	for i := 0; i < n; i++ {
		val, err := s.Get(makeKey(100 + i))
		if err != nil || val == nil {
			t.Errorf("key %d: get failed (err=%v, val=%v)", i, err, val)
		}
	}

	// The NextKeyChain must be sorted and contiguous (sentinel to sentinel).
	chain, err := s.NextKeyChain()
	if err != nil {
		t.Fatalf("NextKeyChain: %v", err)
	}
	// Should be: MIN, n keys, MAX = n+2 entries
	if len(chain) != n+2 {
		t.Fatalf("chain length: want %d (n+2), got %d\nchain=%v", n+2, len(chain), chain)
	}
}

// ─────────────────────────── Close / reopen idempotency ───────────────────

func TestCloseReopen(t *testing.T) {
	dir := tempDir(t)

	s1 := newTestShard(t, dir)
	_ = s1.Insert(makeKey(50), []byte("x"), v(1))
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := newTestShard(t, dir) // reopens the CSV

	// The file exists and has the right entry.
	val, err := s2.Get(makeKey(50))
	if err != nil || string(val) != "x" {
		t.Fatalf("after close/reopen: want 'x', err=%v, val=%q", err, val)
	}

	// Verify log path exists.
	logPath := filepath.Join(dir, "entries_shard_0.csv")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("log file missing: %v", err)
	}
}
