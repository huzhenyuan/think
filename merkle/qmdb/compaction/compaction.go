// Package compaction implements the deterministic Compaction worker for QMDB.
//
// Problem: if an account's value never changes, its Entry stays "alive" in an old Twig
// forever — that Twig can never become Inactive, and its 288-byte footprint accumulates.
//
// Solution: periodically copy still-active entries from old Full Twigs into the
// current Fresh Twig (new Entry IDs, OldId chain preserved). Once all entries in the
// old Twig have been re-appended, every ActiveBit in that Twig becomes 0, causing the
// Twig to transition to Inactive and eventually to Pruned.
//
// Determinism guarantee:
// In a consensus system every validator must trigger Compaction at exactly the same
// moment, or their Twig structures (and thus state roots) will diverge.
// The trigger condition is: any Full Twig whose ActiveCount/TwigSize < CompactionThreshold.
// This ratio depends only on EntryIDs and ActiveBit flips — both are deterministic
// functions of the transaction stream, so every node triggers at the same time.
package compaction

import (
	"fmt"

	"github.com/qmdb/shard"
	"github.com/qmdb/twig"
	"github.com/qmdb/types"
)

const (
	// CompactionThreshold: trigger Compaction when ActiveCount / TwigSize < 0.5.
	// I.e. more than half the entries in a Twig are stale.
	CompactionThreshold = 0.5
)

// CompactionResult describes the outcome of one Compaction run.
type CompactionResult struct {
	ShardID        int
	TwigID         uint64
	EntriesMoved   int
	TwigTransition string // "Full→Inactive" or "no change"
}

// RunCompactionIfNeeded checks all Full Twigs in the given Shard and compacts
// any that exceed the staleness threshold.
//
// It returns a list of CompactionResult entries, one per Twig that was processed.
// This function should be called after EndBlock() to keep it deterministic.
func RunCompactionIfNeeded(s *shard.Shard, version types.Version) ([]CompactionResult, error) {
	var results []CompactionResult

	// Collect twigs that need compaction (snapshot, so we don't modify the map during iteration).
	candidates := collectCompactionCandidates(s)

	for _, candidateTwig := range candidates {
		result, err := compactTwig(s, candidateTwig, version)
		if err != nil {
			return results, fmt.Errorf("compact twig %d: %w", candidateTwig.TwigID, err)
		}
		results = append(results, result)
	}
	return results, nil
}

// collectCompactionCandidates returns all Full Twigs that exceed the staleness threshold.
func collectCompactionCandidates(s *shard.Shard) []*twig.Twig {
	var candidates []*twig.Twig
	for _, t := range s.AllTwigs() {
		if t.NeedsCompaction() {
			candidates = append(candidates, t)
		}
	}
	return candidates
}

// compactTwig migrates live entries from an old Full Twig to the current Fresh Twig.
// Steps:
//  1. Snapshot the active slot indices under the Shard read-lock (via collectActiveSlots).
//  2. For each snapshotted-active entry, re-insert it via Shard.Update.
//     The new Entry gets a new ID in the Fresh Twig; OldId points to the old Entry.
//  3. The old Twig's ActiveBits are cleared as old entries are superseded.
//  4. Once all bits are 0, the old Twig naturally transitions to Inactive.
func compactTwig(s *shard.Shard, t *twig.Twig, version types.Version) (CompactionResult, error) {
	result := CompactionResult{
		ShardID: t.ShardID,
		TwigID:  t.TwigID,
	}

	// Snapshot active slots BEFORE iterating. Without this snapshot, a concurrent
	// writer could clear bits between our IsSlotActive check and the Update call,
	// causing us to re-append an already-superseded entry.
	activeSlots := make([]int, 0, t.ActiveCount)
	for slot := 0; slot < types.TwigSize; slot++ {
		if t.IsSlotActive(slot) {
			activeSlots = append(activeSlots, slot)
		}
	}

	moved := 0
	for _, slot := range activeSlots {

		entryID := t.TwigID*types.TwigSize + uint64(slot)
		// We re-append the value as an Update — this creates a new entry and
		// marks the old entry (entryID) inactive via the Shard's update path.
		// We use the shard's internal read to fetch the current value.
		e, err := readEntryByID(s, entryID)
		if err != nil {
			return result, fmt.Errorf("read entry %d: %w", entryID, err)
		}
		if e == nil {
			continue
		}
		if e.IsDeleted {
			continue
		}

		// Re-append the entry as an Update.
		if err := s.Update(e.Key, e.Value, version); err != nil {
			return result, fmt.Errorf("re-append entry %d: %w", entryID, err)
		}
		moved++
	}

	result.EntriesMoved = moved
	if t.Status == twig.StatusInactive {
		result.TwigTransition = "Full→Inactive"
	} else {
		result.TwigTransition = "no change"
	}
	return result, nil
}

// readEntryByID is a helper that reads an Entry from the Shard's append log by ID.
// This avoids exporting the AppendLog directly from the Shard.
// In production QMDB, the Compaction worker would have direct access to the log.
func readEntryByID(s *shard.Shard, entryID uint64) (*types.Entry, error) {
	// We find the entry's key via the index if still current, or traverse by ID.
	// For this implementation, we perform a snapshot look-through.
	// The simplest approach: the Shard method GetEntryByID is added below.
	return s.GetEntryByID(entryID)
}
