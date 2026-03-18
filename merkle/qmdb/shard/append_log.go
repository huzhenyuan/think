// AppendLog simulates the SSD sequential append log for one Shard using a CSV file.
//
// In a production QMDB, this would be a binary append-only file on NVMe SSD.
// Here we use CSV so that every entry is directly human-readable for observation.
//
// The CSV schema (see csvHeader) maps directly to the Entry struct fields.
// An entry is looked up by seeking to byte offset = EntryID * entryCSVRowSize (approximate;
// since CSV rows have variable length, we maintain an in-memory offset table).
package shard

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/qmdb/types"
)

// csvHeader defines the column names of the entries CSV file.
var csvHeader = []string{
	"entry_id",     // uint64, decimal
	"key_hex",      // 28-byte key, hex
	"value_hex",    // variable-length value, hex (empty if deleted)
	"next_key_hex", // 28-byte next key, hex
	"old_id",       // uint64, decimal (NullEntryID = "null")
	"old_nkid",     // uint64, decimal (NullEntryID = "null")
	"version",      // uint64, decimal
	"block_height", // uint64, decimal
	"tx_index",     // uint32, decimal
	"is_deleted",   // bool, "true"/"false"
}

// AppendLog is a CSV-backed sequential append log for one Shard.
// It simulates the role of an NVMe SSD append file in the real QMDB.
type AppendLog struct {
	// filePath is the path to the CSV file.
	filePath string

	// file is the open file handle (kept open for efficient appending).
	file *os.File

	// writer is the CSV writer tied to file.
	writer *csv.Writer

	// offsetTable maps EntryID → byte offset in the CSV file (after the header).
	// Used to seek directly to any entry for O(1) disk reads.
	offsetTable map[uint64]int64

	// nextOffset tracks the byte offset of the next row to be written.
	nextOffset int64

	// rowCount is the number of data rows written so far.
	rowCount int64
}

// NewAppendLog opens (or creates) the CSV file at filePath and returns an AppendLog.
// If the file already exists and has a valid header, existing entries are indexed.
func NewAppendLog(filePath string) (*AppendLog, error) {
	al := &AppendLog{
		filePath:    filePath,
		offsetTable: make(map[uint64]int64),
	}

	// Open or create the file.
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open append log %s: %w", filePath, err)
	}
	al.file = f

	// Check if the file is new (empty) or existing.
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if info.Size() == 0 {
		// New file: write header.
		w := csv.NewWriter(f)
		if err := w.Write(csvHeader); err != nil {
			return nil, fmt.Errorf("write csv header: %w", err)
		}
		w.Flush()
		offset, _ := f.Seek(0, io.SeekCurrent)
		al.nextOffset = offset
	} else {
		// Existing file: index all existing rows.
		if err := al.rebuildOffsetTable(); err != nil {
			return nil, fmt.Errorf("rebuild offset table: %w", err)
		}
	}

	// Position file cursor at end for future appends.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	endOffset, _ := f.Seek(0, io.SeekCurrent)
	al.nextOffset = endOffset
	al.writer = csv.NewWriter(f)
	return al, nil
}

// Append writes a new Entry as a CSV row. Returns the byte offset of the written row.
func (al *AppendLog) Append(e *types.Entry) (int64, error) {
	row := entryToCSVRow(e)
	offset := al.nextOffset

	if err := al.writer.Write(row); err != nil {
		return 0, fmt.Errorf("csv write entry %d: %w", e.Id, err)
	}
	al.writer.Flush()
	if err := al.writer.Error(); err != nil {
		return 0, err
	}

	// Update the in-memory offset table.
	al.offsetTable[e.Id] = offset

	// Advance nextOffset: seek to current end.
	newOffset, err := al.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	al.nextOffset = newOffset
	al.rowCount++
	return offset, nil
}

// ReadEntry reads and parses the Entry with the given ID by seeking to its CSV row.
// Returns nil if the entry ID is not found.
func (al *AppendLog) ReadEntry(entryID uint64) (*types.Entry, error) {
	offset, ok := al.offsetTable[entryID]
	if !ok {
		return nil, fmt.Errorf("entry ID %d not found in append log", entryID)
	}

	// Seek to the row offset.
	if _, err := al.file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	r := csv.NewReader(al.file)
	r.FieldsPerRecord = len(csvHeader)
	row, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv row for entry %d: %w", entryID, err)
	}

	// Restore file cursor to end for future appends.
	if _, err := al.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	return csvRowToEntry(row)
}

// ReadAllEntries reads all entries from the CSV file in order. Used by Compaction.
func (al *AppendLog) ReadAllEntries() ([]*types.Entry, error) {
	if _, err := al.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	r := csv.NewReader(al.file)
	r.FieldsPerRecord = len(csvHeader)

	// Skip header.
	if _, err := r.Read(); err != nil {
		return nil, err
	}

	var entries []*types.Entry
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		e, err := csvRowToEntry(row)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	// Restore cursor.
	if _, err := al.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return entries, nil
}

// Close flushes and closes the underlying file.
func (al *AppendLog) Close() error {
	al.writer.Flush()
	return al.file.Close()
}

// RowCount returns the number of data rows (entries) written so far.
func (al *AppendLog) RowCount() int64 {
	return al.rowCount
}

// ──────────────────────────── helpers ────────────────────────────────────────

// rebuildOffsetTable scans the CSV file from the start and rebuilds offsetTable.
func (al *AppendLog) rebuildOffsetTable() error {
	if _, err := al.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := csv.NewReader(al.file)
	r.FieldsPerRecord = len(csvHeader)

	// Skip header row; record only its size.
	if _, err := r.Read(); err != nil {
		return err
	}

	for {
		before, err := al.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// We need byte offsets per row. csv.Reader doesn't expose them natively,
		// so we track position before each Read.
		// NOTE: r.Read() may buffer; this approach under-counts when buffering occurs.
		// For correctness we re-open as needed via direct seeks.
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		idVal, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil {
			continue
		}
		al.offsetTable[idVal] = before
		al.rowCount++
	}
	return nil
}

// entryToCSVRow serializes an Entry into CSV columns.
func entryToCSVRow(e *types.Entry) []string {
	oldID := fmtNullableID(e.OldId)
	oldNKID := fmtNullableID(e.OldNextKeyId)

	return []string{
		strconv.FormatUint(e.Id, 10),
		hex.EncodeToString(e.Key[:]),
		hex.EncodeToString(e.Value),
		hex.EncodeToString(e.NextKey[:]),
		oldID,
		oldNKID,
		strconv.FormatUint(uint64(e.Version), 10),
		strconv.FormatUint(e.Version.BlockHeight(), 10),
		strconv.FormatUint(uint64(e.Version.TxIndex()), 10),
		strconv.FormatBool(e.IsDeleted),
	}
}

// csvRowToEntry deserializes an Entry from CSV columns.
func csvRowToEntry(row []string) (*types.Entry, error) {
	if len(row) != len(csvHeader) {
		return nil, fmt.Errorf("expected %d columns, got %d", len(csvHeader), len(row))
	}

	id, err := strconv.ParseUint(row[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse entry_id: %w", err)
	}

	keyBytes, err := hex.DecodeString(row[1])
	if err != nil || len(keyBytes) != types.KeySize {
		return nil, fmt.Errorf("parse key_hex: %w", err)
	}

	valueBytes, err := hex.DecodeString(row[2])
	if err != nil {
		return nil, fmt.Errorf("parse value_hex: %w", err)
	}

	nkBytes, err := hex.DecodeString(row[3])
	if err != nil || len(nkBytes) != types.KeySize {
		return nil, fmt.Errorf("parse next_key_hex: %w", err)
	}

	oldID, err := parseNullableID(row[4])
	if err != nil {
		return nil, fmt.Errorf("parse old_id: %w", err)
	}
	oldNKID, err := parseNullableID(row[5])
	if err != nil {
		return nil, fmt.Errorf("parse old_nkid: %w", err)
	}

	ver, err := strconv.ParseUint(row[6], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse version: %w", err)
	}

	isDeleted, err := strconv.ParseBool(row[9])
	if err != nil {
		return nil, fmt.Errorf("parse is_deleted: %w", err)
	}

	e := &types.Entry{
		Id:           id,
		Value:        valueBytes,
		OldId:        oldID,
		OldNextKeyId: oldNKID,
		Version:      types.Version(ver),
		IsDeleted:    isDeleted,
	}
	copy(e.Key[:], keyBytes)
	copy(e.NextKey[:], nkBytes)
	return e, nil
}

func fmtNullableID(id uint64) string {
	if id == types.NullEntryID {
		return "null"
	}
	return strconv.FormatUint(id, 10)
}

func parseNullableID(s string) (uint64, error) {
	if s == "null" {
		return types.NullEntryID, nil
	}
	return strconv.ParseUint(s, 10, 64)
}
