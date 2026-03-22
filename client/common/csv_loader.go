package common

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// nombre, apellido, documento, nacimiento, numero
const csvFieldCount = 5

func LoadBetsFromCSV(path string, agencyID string) ([][6]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.TrimLeadingSpace = true

	var rows [][6]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		if len(rec) < csvFieldCount {
			return nil, fmt.Errorf("row has too few columns (expected %d, got %d)", csvFieldCount, len(rec))
		}
		row := [6]string{
			agencyID,
			strings.TrimSpace(rec[0]),
			strings.TrimSpace(rec[1]),
			strings.TrimSpace(rec[2]),
			strings.TrimSpace(rec[3]),
			strings.TrimSpace(rec[4]),
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no data rows in %s", path)
	}
	return rows, nil
}
