package report

import (
	"strings"
	"testing"

	"github.com/mk/ccloud-rbac/internal/model"
)

func TestRender(t *testing.T) {
	r := &model.Report{
		GeneratedAt: "2026-06-09T00:00:00Z",
		OrgID:       "org-1",
		OrgName:     "Test </script> Org", // must not break out of the script block
		Bindings: []model.Binding{
			{ID: "rb-1", PrincipalID: "u-1", PrincipalName: "Alice", Role: "OrganizationAdmin", Access: "admin"},
		},
	}
	out, err := Render(r)
	if err != nil {
		t.Fatal(err)
	}
	html := string(out)
	if strings.Contains(html, "__REPORT_JSON__") {
		t.Error("marker not replaced")
	}
	if !strings.Contains(html, `"orgId":"org-1"`) {
		t.Error("report JSON not embedded")
	}
	// The org name contains "</script>"; json.Marshal must have escaped it
	// so the data block is not terminated early.
	if strings.Contains(html, "Test </script> Org") {
		t.Error("unescaped </script> inside embedded JSON")
	}
}
