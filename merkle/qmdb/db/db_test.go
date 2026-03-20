package db_test

import (
	"os"
	"testing"

	"github.com/qmdb/db"
	"github.com/qmdb/types"
)

// ─────────────────────────── helpers ────────────────────────────────────────

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "qmdb_db_test_*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func openDB(t *testing.T, dir string) *db.QMDB {
	t.Helper()
	d, err := db.Open(dir)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	return d
}

func closeDB(t *testing.T, d *db.QMDB) {
	t.Helper()
	if err := d.Close(); err != nil {
		t.Fatalf("db.Close: %v", err)
	}
}

// ─────────────────────────── basic operations ────────────────────────────────

func TestSetGet(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	d.BeginBlock(1)
	if err := d.Set([]byte("alice"), []byte("100")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, err := d.Get([]byte("alice"))
	if err != nil || string(val) != "100" {
		t.Fatalf("Get: err=%v val=%q", err, val)
	}
}

func TestSetGetDelete(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	d.BeginBlock(1)
	_ = d.Set([]byte("bob"), []byte("200"))
	_ = d.Delete([]byte("bob"))

	val, _ := d.Get([]byte("bob"))
	if val != nil {
		t.Fatalf("want nil after delete, got %q", val)
	}
}

func TestSetOverwrite(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	d.BeginBlock(1)
	_ = d.Set([]byte("key"), []byte("v1"))
	d.BeginBlock(2)
	_ = d.Set([]byte("key"), []byte("v2"))

	val, _ := d.Get([]byte("key"))
	if string(val) != "v2" {
		t.Fatalf("want v2, got %q", val)
	}
}

// ─────────────────────────── GetAtVersion ─────────────────────────────────

func TestGetAtVersion(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	d.BeginBlock(1)
	_ = d.Set([]byte("acct"), []byte("init"))

	d.BeginBlock(3)
	_ = d.Set([]byte("acct"), []byte("updated"))

	v1 := types.NewVersion(1, 0)
	v2 := types.NewVersion(2, 0) // between v1 and v3
	v3 := types.NewVersion(3, 0)

	val, _ := d.GetAtVersion([]byte("acct"), v1)
	if string(val) != "init" {
		t.Fatalf("v1: want 'init', got %q", val)
	}
	val, _ = d.GetAtVersion([]byte("acct"), v2)
	if string(val) != "init" {
		t.Fatalf("v2: want 'init', got %q", val)
	}
	val, _ = d.GetAtVersion([]byte("acct"), v3)
	if string(val) != "updated" {
		t.Fatalf("v3: want 'updated', got %q", val)
	}
}

// ─────────────────────────── StateRoot changes ───────────────────────────

func TestStateRootChanges(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	root0 := d.StateRoot()

	d.BeginBlock(1)
	_ = d.Set([]byte("x"), []byte("1"))
	root1 := d.StateRoot()

	if root0 == root1 {
		t.Fatal("state root should change after write")
	}

	root1b := d.EndBlock()
	if root1 != root1b {
		t.Fatal("EndBlock root should equal StateRoot after writes for the same block")
	}
}

// ─────────────────────────── Merkle proof ─────────────────────────────────

func TestMerkleProofFreshTwig(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	d.BeginBlock(1)
	key := "proof_test_key"
	_ = d.Set([]byte(key), []byte("proof_value"))

	proof, err := d.ProofForKey([]byte(key))
	if err != nil {
		t.Fatalf("ProofForKey: %v", err)
	}

	// Proof should verify successfully.
	ok := proof.Verify(d.StateRoot())
	if !ok {
		t.Fatalf("Verify: proof failed")
	}
}

func TestNonExistenceProof(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	d.BeginBlock(1)
	_ = d.Set([]byte("aaa"), []byte("v_aaa"))
	_ = d.Set([]byte("zzz"), []byte("v_zzz"))

	// FindPredecessorEntry returns the predecessor for a non-existent key.
	// predecessor.Key < hash("mmm") ≤ predecessor.NextKey proves non-existence.
	pred, err := d.FindPredecessorEntry([]byte("mmm"))
	if err != nil {
		t.Fatalf("FindPredecessorEntry: %v", err)
	}
	if pred == nil {
		t.Fatal("predecessor should not be nil")
	}
	if pred.Key == pred.NextKey {
		t.Fatal("predecessor's Key and NextKey must differ for a valid non-existence proof")
	}
}

// ─────────────────────────── Persistence / recovery ─────────────────────

func TestRecovery(t *testing.T) {
	dir := tempDir(t)

	// Session 1: write several keys across different blocks.
	{
		d := openDB(t, dir)
		d.BeginBlock(1)
		_ = d.Set([]byte("p1"), []byte("val1"))
		_ = d.Set([]byte("p2"), []byte("val2"))
		_ = d.EndBlock()

		d.BeginBlock(2)
		_ = d.Set([]byte("p1"), []byte("val1_v2"))
		_ = d.Delete([]byte("p2"))
		_ = d.EndBlock()

		closeDB(t, d)
	}

	// Session 2: reopen and verify state.
	d2 := openDB(t, dir)
	defer closeDB(t, d2)

	val, err := d2.Get([]byte("p1"))
	if err != nil || string(val) != "val1_v2" {
		t.Fatalf("recovery p1: err=%v val=%q", err, val)
	}
	val, _ = d2.Get([]byte("p2"))
	if val != nil {
		t.Fatalf("recovery p2: want nil (deleted), got %q", val)
	}

	// GetAtVersion should still work on recovered data.
	val, _ = d2.GetAtVersion([]byte("p1"), types.NewVersion(1, 0))
	if string(val) != "val1" {
		t.Fatalf("recovery history: want 'val1', got %q", val)
	}
}

// ─────────────────────────── Multiple blocks / versions ──────────────────

func TestMultiBlockVersions(t *testing.T) {
	d := openDB(t, tempDir(t))
	defer closeDB(t, d)

	for b := uint64(1); b <= 5; b++ {
		d.BeginBlock(b)
		_ = d.Set([]byte("counter"), []byte{byte(b * 10)})
		d.EndBlock()
	}

	// Each block version should return the corresponding value.
	for b := uint64(1); b <= 5; b++ {
		val, err := d.GetAtVersion([]byte("counter"), types.NewVersion(b, 0))
		if err != nil {
			t.Errorf("block %d GetAtVersion: %v", b, err)
			continue
		}
		if len(val) == 0 || val[0] != byte(b*10) {
			t.Errorf("block %d: want %d, got %v", b, b*10, val)
		}
	}
}
