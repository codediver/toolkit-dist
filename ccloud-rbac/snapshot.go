package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/mk/ccloud-rbac/internal/ccloud"
)

// writeSnapshotJSON dumps the raw fetched data for debugging or further
// processing with other tools (e.g. jq).
func writeSnapshotJSON(path string, snap *ccloud.Snapshot) error {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
