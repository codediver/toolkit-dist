// Package report renders the enriched RBAC model into a self-contained
// interactive HTML file (no external assets, vanilla JS).
package report

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mk/ccloud-rbac/internal/model"
)

//go:embed template.html
var templateHTML string

const jsonMarker = "__REPORT_JSON__"

// Render returns the complete HTML document with the report model embedded
// as JSON.
func Render(r *model.Report) ([]byte, error) {
	// json.Marshal HTML-escapes "<", ">" and "&" as unicode escapes by
	// default, so the embedded payload cannot terminate the surrounding
	// script block early.
	data, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal report: %w", err)
	}
	if !strings.Contains(templateHTML, jsonMarker) {
		return nil, fmt.Errorf("template is missing the %s marker", jsonMarker)
	}
	return []byte(strings.Replace(templateHTML, jsonMarker, string(data), 1)), nil
}
