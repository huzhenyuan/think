// Package observe provides real-time observability for QMDB.
//
// After every state-mutating operation, an Observer dumps 6 CSV files:
//
//	entries_shard_N.csv      — append log per Shard (updated by the Shard itself)
//	index_shard_N.csv        — B-tree index snapshot per Shard
//	twig_registry.csv        — all Twigs (all shards)
//	fresh_twig_shard_N.csv   — Fresh Twig leaves + internal Merkle nodes
//	upper_tree.csv           — upper Merkle tree nodes
//	global_state.csv         — state root history (appended, not overwritten)
//
// These files can be opened in any spreadsheet / tail -f in a terminal while
// running main.go to watch the state evolve step by step.
package observe

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/qmdb/crypto"
	"github.com/qmdb/db"
	"github.com/qmdb/shard"
	"github.com/qmdb/types"
)

// Observer implements db.ObserverHook and dumps CSV files after every write.
type Observer struct {
	dataDir   string
	traceFile *os.File
	traceCSV  *csv.Writer
	traceSeq  int // monotonically increasing sub-step counter
	opSeq     int // index of the current high-level operation (AfterWrite call)
}

// NewObserver creates an Observer that writes CSV files to dataDir.
func NewObserver(dataDir string) *Observer {
	o := &Observer{dataDir: dataDir}

	path := dataDir + "/trace.csv"
	f, err := os.Create(path)
	if err != nil {
		return o
	}
	o.traceFile = f
	o.traceCSV = csv.NewWriter(f)
	_ = o.traceCSV.Write([]string{
		"seq",        // global sub-step index
		"op_seq",     // which high-level operation (AfterWrite call index)
		"op",         // high-level op: Set / Delete / Init
		"sub_op",     // fine-grained action (see constants below)
		"shard_id",   // affected shard
		"key_hex",    // 56-char hex of 28-byte key
		"entry_id",   // entry ID involved, or ""
		"old_id",     // prev entry ID (for supersede events)
		"twig_id",    // twig affected
		"slot",       // slot within the twig
		"block",      // block height
		"tx",         // tx index
		"hash_hex",   // new Merkle hash (Twig root / UpperTree root / leaf hash)
		"detail",     // human-readable description (English)
	})
	o.traceCSV.Flush()
	return o
}

// AfterWrite is called by QMDB after every mutating operation.
func (o *Observer) AfterWrite(qmdb *db.QMDB, op string, key [types.KeySize]byte, version types.Version) {
	shardID := int(key[0] >> 4)

	// 1. Index snapshot for the affected Shard.
	o.dumpIndex(qmdb, shardID)

	// 2. Fresh Twig snapshot for the affected Shard.
	o.dumpFreshTwig(qmdb, shardID)

	// 3. Twig registry (all shards, all twigs).
	o.dumpTwigRegistry(qmdb)

	// 4. Upper tree.
	o.dumpUpperTree(qmdb)

	// 5. Append one row to the global state log.
	o.appendGlobalState(qmdb, op, key, version)

	o.opSeq++
}

// DumpAll writes all 6 CSV categories for all shards (useful at start-up / checkpoints).
func (o *Observer) DumpAll(qmdb *db.QMDB) {
	for shardID := 0; shardID < types.ShardCount; shardID++ {
		o.dumpIndex(qmdb, shardID)
		o.dumpFreshTwig(qmdb, shardID)
	}
	o.dumpTwigRegistry(qmdb)
	o.dumpUpperTree(qmdb)
}

// ─────────────────────────────────────────────────────────────────────────────
// CSV dump functions
// ─────────────────────────────────────────────────────────────────────────────

// dumpIndex writes the B-tree index snapshot for one Shard.
// File: index_shard_N.csv
// Columns: key_prefix_hex, full_key_hex, entry_id
func (o *Observer) dumpIndex(qmdb *db.QMDB, shardID int) {
	path := fmt.Sprintf("%s/index_shard_%d.csv", o.dataDir, shardID)
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	_ = w.Write([]string{"key_prefix_hex", "full_key_hex", "entry_id"})

	for _, ie := range qmdb.Shard(shardID).IndexSnapshot() {
		_ = w.Write([]string{
			hex.EncodeToString(ie.KeyPrefix[:]),
			hex.EncodeToString(ie.FullKey[:]),
			strconv.FormatUint(ie.EntryID, 10),
		})
	}
	w.Flush()
}

// dumpFreshTwig writes the current Fresh Twig's leaf hashes and Merkle nodes.
// File: fresh_twig_shard_N.csv
// Columns: type, index, slot_or_level, hash_hex, active
func (o *Observer) dumpFreshTwig(qmdb *db.QMDB, shardID int) {
	path := fmt.Sprintf("%s/fresh_twig_shard_%d.csv", o.dataDir, shardID)
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	_ = w.Write([]string{"type", "heap_index", "slot_or_level", "hash_hex", "description"})

	ft := qmdb.Shard(shardID).FreshTwigSnapshot()
	if ft == nil || ft.Fresh == nil {
		w.Flush()
		return
	}

	// Leaf nodes: heap indices TwigSize..2*TwigSize-1
	for slot := 0; slot < types.TwigSize; slot++ {
		heapIdx := types.TwigSize + slot
		h := ft.Fresh.Nodes[heapIdx]
		if h == crypto.NullHash {
			continue // skip empty slots for readability
		}
		_ = w.Write([]string{
			"leaf",
			strconv.Itoa(heapIdx),
			strconv.Itoa(slot),
			hex.EncodeToString(h[:]),
			fmt.Sprintf("leaf_slot_%d", slot),
		})
	}

	// Internal nodes: heap indices 1..TwigSize-1
	for idx := 1; idx < types.TwigSize; idx++ {
		h := ft.Fresh.Nodes[idx]
		if h == crypto.NullHash {
			continue
		}
		level := 0
		for tmp := idx; tmp > 1; tmp >>= 1 {
			level++
		}
		desc := "internal"
		if idx == 1 {
			desc = "twig_root"
		}
		_ = w.Write([]string{
			desc,
			strconv.Itoa(idx),
			strconv.Itoa(level),
			hex.EncodeToString(h[:]),
			fmt.Sprintf("level_%d_node_%d", level, idx),
		})
	}
	w.Flush()
}

// dumpTwigRegistry writes the state of every Twig across all Shards.
// File: twig_registry.csv
// Columns: shard_id, twig_id, status, active_count, root_hash_hex, needs_compaction
func (o *Observer) dumpTwigRegistry(qmdb *db.QMDB) {
	path := fmt.Sprintf("%s/twig_registry.csv", o.dataDir)
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	_ = w.Write([]string{"shard_id", "twig_id", "status", "active_count", "root_hash_hex", "needs_compaction"})

	for shardID := 0; shardID < types.ShardCount; shardID++ {
		s := qmdb.Shard(shardID)

		// Fresh Twig.
		ft := s.FreshTwigSnapshot()
		if ft != nil {
			activeCount := 0
			if ft.Fresh != nil {
				activeCount = ft.Fresh.NextSlot
			}
			_ = w.Write([]string{
				strconv.Itoa(shardID),
				strconv.FormatUint(ft.TwigID, 10),
				ft.Status.String(),
				strconv.Itoa(activeCount),
				hex.EncodeToString(ft.RootHash[:]),
				"false",
			})
		}

		// Full / Inactive Twigs.
		for twigID, t := range s.AllTwigs() {
			_ = w.Write([]string{
				strconv.Itoa(shardID),
				strconv.FormatUint(twigID, 10),
				t.Status.String(),
				strconv.Itoa(t.ActiveCount),
				hex.EncodeToString(t.RootHash[:]),
				strconv.FormatBool(t.NeedsCompaction()),
			})
		}
	}
	w.Flush()
}

// dumpUpperTree writes all non-NullHash nodes in the upper Merkle tree.
// File: upper_tree.csv
// Columns: heap_index, left_child, right_child, hash_hex
func (o *Observer) dumpUpperTree(qmdb *db.QMDB) {
	path := fmt.Sprintf("%s/upper_tree.csv", o.dataDir)
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	_ = w.Write([]string{"heap_index", "left_child", "right_child", "hash_hex"})

	for _, node := range qmdb.UpperTree().AllNodes() {
		_ = w.Write([]string{
			strconv.Itoa(node.Index),
			strconv.Itoa(node.LeftChild),
			strconv.Itoa(node.RightChild),
			hex.EncodeToString(node.Hash[:]),
		})
	}
	w.Flush()
}

// appendGlobalState appends one row to the global state log.
// File: global_state.csv (append mode; never overwritten)
// Columns: timestamp, block_height, tx_index, operation, key_hex, shard_id, state_root_hex
func (o *Observer) appendGlobalState(qmdb *db.QMDB, op string, key [types.KeySize]byte, version types.Version) {
	path := fmt.Sprintf("%s/global_state.csv", o.dataDir)

	// Check if header is needed.
	needsHeader := false
	if _, err := os.Stat(path); os.IsNotExist(err) {
		needsHeader = true
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if needsHeader {
		_ = w.Write([]string{
			"timestamp", "block_height", "tx_index",
			"operation", "key_hex", "shard_id", "state_root_hex",
		})
	}

	root := qmdb.StateRoot()
	_ = w.Write([]string{
		time.Now().Format(time.RFC3339Nano),
		strconv.FormatUint(version.BlockHeight(), 10),
		strconv.FormatUint(uint64(version.TxIndex()), 10),
		op,
		hex.EncodeToString(key[:]),
		strconv.Itoa(int(key[0] >> 4)),
		hex.EncodeToString(root[:]),
	})
	w.Flush()
}

// ─────────────────────────────────────────────────────────────────────────────
// Trace helpers — fine-grained sub-step logging
// ─────────────────────────────────────────────────────────────────────────────

// AppendTrace records one fine-grained sub-step to trace.csv.
// Called by the Shard (via db layer) at every meaningful internal action.
//
// sub_op values used by callers:
//   - "btree_lookup"       B-Tree key lookup (read)
//   - "find_predecessor"   locate predecessor in sorted list (read)
//   - "log_append"         append new Entry to CSV log (write)
//   - "leaf_hash"          compute Keccak256 leaf hash for Entry
//   - "twig_rehash"        recompute internal Merkle node(s) inside Fresh Twig
//   - "twig_root_update"   Fresh Twig root changes → notify UpperTree
//   - "upper_tree_update"  UpperTree propagates new Twig root toward global root
//   - "btree_update"       B-Tree index entry inserted/updated/deleted
//   - "mark_inactive"      entry superseded; ActiveBit cleared
//   - "state_root"         final global state root after operation completes
func (o *Observer) AppendTrace(
	op string,
	subOp string,
	shardID int,
	keyHex string,
	entryID string,
	oldID string,
	twigID string,
	slot string,
	block string,
	tx string,
	hashHex string,
	detail string,
) {
	if o.traceCSV == nil {
		return
	}
	_ = o.traceCSV.Write([]string{
		strconv.Itoa(o.traceSeq),
		strconv.Itoa(o.opSeq),
		op,
		subOp,
		strconv.Itoa(shardID),
		keyHex,
		entryID,
		oldID,
		twigID,
		slot,
		block,
		tx,
		hashHex,
		detail,
	})
	o.traceCSV.Flush()
	o.traceSeq++
}

// MakeTraceHook returns a shard.TraceHook that forwards every sub-step to this Observer's trace log.
// Install it on all shards via qmdb.SetShardTraceHooks(obs.MakeTraceHook()).
func (o *Observer) MakeTraceHook() shard.TraceHook {
	return func(op, subOp, keyHex, entryID, oldID, twigID, slot, block, tx, hashHex, detail string) {
		shardID := 0
		if len(keyHex) >= 2 {
			b, _ := strconv.ParseUint(keyHex[:2], 16, 8)
			shardID = int(b >> 4)
		}
		o.AppendTrace(op, subOp, shardID, keyHex, entryID, oldID, twigID, slot, block, tx, hashHex, detail)
	}
}

// Close flushes and closes the trace file.
func (o *Observer) Close() {
	if o.traceCSV != nil {
		o.traceCSV.Flush()
	}
	if o.traceFile != nil {
		_ = o.traceFile.Close()
	}
}
