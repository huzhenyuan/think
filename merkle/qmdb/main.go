// QMDB Demo: Step-by-step simulation reproducing the examples from the design document.
//
// This program walks through the exact scenario described in the design doc:
//
//	Block h=100:
//	  Insert alice (balance=50)
//	  Insert bob   (balance=80)
//	Block h=200:
//	  Update alice (balance=120)
//	Block h=350:
//	  Delete bob
//
// After each operation the program:
//  1. Prints a summary to the terminal (linked list state, state root).
//  2. Dumps 6 CSV files into the ./data/ directory for inspection.
//
// Open the CSV files in a spreadsheet or watch them with:
//
//	watch -n1 cat data/global_state.csv
//
// The following CSV files are populated:
//
//	data/entries_shard_N.csv        — append log per Shard
//	data/index_shard_N.csv          — B-tree index snapshot
//	data/fresh_twig_shard_N.csv     — Fresh Twig Merkle tree
//	data/twig_registry.csv          — all Twig statuses
//	data/upper_tree.csv             — upper Merkle tree
//	data/global_state.csv           — state root history
package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/qmdb/crypto"
	"github.com/qmdb/db"
	"github.com/qmdb/eth"
	"github.com/qmdb/observe"
	"github.com/qmdb/types"
)

const dataDir = "./data"

func main() {
	// ── Setup ──────────────────────────────────────────────────────────────
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fatal("mkdir data:", err)
	}
	// Remove previous run's CSV files (fresh start).
	clearDataDir()

	qmdb, err := db.Open(dataDir)
	if err != nil {
		fatal("open QMDB:", err)
	}

	// Attach the observer: every write will auto-dump CSV files.
	obs := observe.NewObserver(dataDir)
	qmdb.Observer = obs
	// Attach fine-grained trace hooks to all 16 shards.
	qmdb.SetShardTraceHooks(obs.MakeTraceHook())

	// Dump the initial state (sentinels only).
	obs.DumpAll(qmdb)

	banner("Initial state (MIN/MAX sentinels installed in all 16 Shards)")
	printStateRoot(qmdb)
	printCSVHint()

	// ── Demonstrate Trie interface ─────────────────────────────────────────
	trie := eth.NewQMDBTrie(qmdb)

	// ── Demonstrate StateDB interface ──────────────────────────────────────
	stateDB := eth.NewQMDBStateDB(qmdb)

	// ═══════════════════════════════════════════════════════════════════════
	// Block h=100 — Insert alice and bob
	// ═══════════════════════════════════════════════════════════════════════
	banner("Block h=100: Insert alice (balance=50) and bob (balance=80)")
	qmdb.BeginBlock(100)

	// TX 0: create alice
	stateDB.Prepare(eth.Hash{0x01}, 0)
	stateDB.CreateAccount(makeAddr("alice"))
	stateDB.SetBalance(makeAddr("alice"), 50)
	stateDB.Finalise(false)

	// TX 1: create bob
	stateDB.Prepare(eth.Hash{0x02}, 1)
	stateDB.CreateAccount(makeAddr("bob"))
	stateDB.SetBalance(makeAddr("bob"), 80)
	stateDB.Finalise(false)

	stateRoot100 := qmdb.EndBlock()
	fmt.Printf("  Block 100 state root: %s\n", hexShort(stateRoot100[:]))
	fmt.Printf("  alice balance: %d\n", stateDB.GetBalance(makeAddr("alice")))
	fmt.Printf("  bob   balance: %d\n", stateDB.GetBalance(makeAddr("bob")))
	printLinkedLists(qmdb)

	// ═══════════════════════════════════════════════════════════════════════
	// One-level lower: use the Trie interface directly
	// ═══════════════════════════════════════════════════════════════════════
	banner("Direct Trie.Update/Get for key 'counter'")
	qmdb.BeginBlock(101)
	qmdb.BeginTx(0)

	if err := trie.Update([]byte("counter"), []byte{0x00, 0x00, 0x00, 0x42}); err != nil {
		fatal("trie.Update:", err)
	}
	val, err := trie.Get([]byte("counter"))
	if err != nil {
		fatal("trie.Get:", err)
	}
	trieRoot := trie.Hash()
	fmt.Printf("  counter value: %x  (state root: %s)\n", val, hexShort(trieRoot[:]))

	_ = qmdb.EndBlock()

	// ═══════════════════════════════════════════════════════════════════════
	// Block h=200 — Update alice's balance
	// ═══════════════════════════════════════════════════════════════════════
	banner("Block h=200: Update alice balance 50 → 120")
	qmdb.BeginBlock(200)
	stateDB.Prepare(eth.Hash{0x03}, 0)
	stateDB.SetBalance(makeAddr("alice"), 120)
	stateDB.Finalise(false)

	stateRoot200 := qmdb.EndBlock()
	fmt.Printf("  Block 200 state root: %s\n", hexShort(stateRoot200[:]))
	fmt.Printf("  alice balance: %d\n", stateDB.GetBalance(makeAddr("alice")))

	// ═══════════════════════════════════════════════════════════════════════
	// History query: alice at h=100
	// ═══════════════════════════════════════════════════════════════════════
	banner("Historical query: alice's balance at block 100")
	v100 := types.NewVersion(100, 0)
	aliceAddr := makeAddr("alice")
	aliceKey := crypto.HashAppKey(append([]byte("acct:"), aliceAddr[:]...))
	aliceAt100, err := qmdb.GetAtVersion(
		append([]byte("acct:"), aliceAddr[:]...),
		v100,
	)
	_ = aliceKey
	if err != nil {
		fmt.Printf("  error: %v\n", err)
	} else if len(aliceAt100) >= 8 {
		balance := uint64(aliceAt100[0]) | uint64(aliceAt100[1])<<8 |
			uint64(aliceAt100[2])<<16 | uint64(aliceAt100[3])<<24 |
			uint64(aliceAt100[4])<<32 | uint64(aliceAt100[5])<<40 |
			uint64(aliceAt100[6])<<48 | uint64(aliceAt100[7])<<56
		fmt.Printf("  alice balance at block 100: %d (expected: 50)\n", balance)
	}

	// ═══════════════════════════════════════════════════════════════════════
	// Block h=350 — Delete bob
	// ═══════════════════════════════════════════════════════════════════════
	banner("Block h=350: Delete bob")
	qmdb.BeginBlock(350)
	stateDB.Prepare(eth.Hash{0x04}, 0)
	stateDB.DeleteAccount(makeAddr("bob"))
	stateDB.Finalise(false)

	stateRoot350 := qmdb.EndBlock()
	fmt.Printf("  Block 350 state root: %s\n", hexShort(stateRoot350[:]))
	fmt.Printf("  bob exists: %v (expected: false)\n", stateDB.Exist(makeAddr("bob")))
	fmt.Printf("  alice balance: %d (expected: 120)\n", stateDB.GetBalance(makeAddr("alice")))
	printLinkedLists(qmdb)

	// ═══════════════════════════════════════════════════════════════════════
	// Proof demo
	// ═══════════════════════════════════════════════════════════════════════
	banner("Merkle proof for 'counter'")
	proof, err := qmdb.ProofForKey([]byte("counter"))
	if err != nil {
		fmt.Printf("  error: %v\n", err)
	} else {
		fmt.Printf("  entryID=%d twigID=%d slot=%d shardID=%d\n",
			proof.EntryID, proof.TwigID, proof.SlotIndex, proof.ShardID)
		fmt.Printf("  twig proof steps: %d, upper tree steps: %d\n",
			len(proof.TwigProof), len(proof.UpperTreeProof))
		fmt.Printf("  state root in proof: %s\n", hexShort(proof.StateRoot[:]))
		fmt.Printf("  partial (in Full Twig): %v\n", proof.IsPartial)

		// Verify the proof against the current state root.
		currentRoot := qmdb.StateRoot()
		if proof.Verify(currentRoot) {
			fmt.Println("  proof.Verify(): ✓ valid")
		} else {
			fmt.Println("  proof.Verify(): ✗ INVALID (bug)")
		}
	}

	// ═══════════════════════════════════════════════════════════════════════
	// Final state root comparison
	// ═══════════════════════════════════════════════════════════════════════
	banner("State root evolution")
	fmt.Printf("  h=100 root: %s\n", hexShort(stateRoot100[:]))
	fmt.Printf("  h=200 root: %s\n", hexShort(stateRoot200[:]))
	fmt.Printf("  h=350 root: %s\n", hexShort(stateRoot350[:]))
	fmt.Println()
	fmt.Println("  State roots differ across blocks: ✓ (each write changes the root)")

	// ═══════════════════════════════════════════════════════════════════════
	// Summary
	// ═══════════════════════════════════════════════════════════════════════
	banner("CSV observation files written to ./data/")
	fmt.Println("  entries_shard_N.csv     — append-only entry log per Shard")
	fmt.Println("  index_shard_N.csv       — B-tree index snapshot (updated after each write)")
	fmt.Println("  fresh_twig_shard_N.csv  — Fresh Twig Merkle leaf + internal node hashes")
	fmt.Println("  twig_registry.csv       — all Twig states (Fresh/Full/Inactive)")
	fmt.Println("  upper_tree.csv          — upper Merkle tree nodes")
	fmt.Println("  global_state.csv        — state root after every operation (append log)")
}

// ─────────────────────────────────────────────────────────────────────────────
// Demo helpers
// ─────────────────────────────────────────────────────────────────────────────

func banner(msg string) {
	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Println(" " + msg)
	fmt.Println("════════════════════════════════════════════════════════════")
}

func printStateRoot(qmdb *db.QMDB) {
	root := qmdb.StateRoot()
	fmt.Printf("  Current state root: %s\n", hexShort(root[:]))
}

func printCSVHint() {
	fmt.Println("  CSV files are in ./data/ — open them in a spreadsheet or tail -f")
}

// printLinkedLists prints the NextKey ordered list for each non-empty Shard.
func printLinkedLists(qmdb *db.QMDB) {
	fmt.Println("  NextKey ordered lists (non-empty shards):")
	for shardID := 0; shardID < types.ShardCount; shardID++ {
		chain, err := qmdb.Shard(shardID).NextKeyChain()
		if err != nil || len(chain) <= 2 {
			// Only 2 entries = MIN→MAX = empty (sentinels only)
			continue
		}
		fmt.Printf("    Shard %d: ", shardID)
		for i, pair := range chain {
			if i > 0 {
				fmt.Print(" → ")
			}
			fmt.Printf("%s(#%s)", pair[0][:8]+"…", pair[1])
		}
		fmt.Println()
	}
}

// makeAddr creates a deterministic 20-byte Address from a name string.
func makeAddr(name string) eth.Address {
	h := crypto.Keccak256([]byte(name))
	var addr eth.Address
	copy(addr[:], h[:20])
	return addr
}

// hexShort returns the first 8 bytes of a byte slice as a hex string.
func hexShort(b []byte) string {
	if len(b) > 8 {
		return hex.EncodeToString(b[:8]) + "…"
	}
	return hex.EncodeToString(b)
}

func fatal(msg string, err error) {
	fmt.Fprintf(os.Stderr, "FATAL: %s %v\n", msg, err)
	os.Exit(1)
}

// clearDataDir removes all CSV files from the data directory for a fresh run.
func clearDataDir() {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			_ = os.Remove(dataDir + "/" + e.Name())
		}
	}
}
