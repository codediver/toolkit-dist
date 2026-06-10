package ccloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFetchRoleBindingsWildcardFallback simulates an API that rejects the
// org-level wildcard crn_pattern: the fetcher must fall back to querying
// each environment subtree and de-duplicate the results.
func TestFetchRoleBindingsWildcardFallback(t *testing.T) {
	const orgCRN = "crn://confluent.cloud/organization=org-1"
	orgBinding := RoleBinding{ID: "rb-org", Principal: "User:u-1", RoleName: "OrganizationAdmin", CRNPattern: orgCRN}
	envBinding := RoleBinding{ID: "rb-env", Principal: "User:u-2", RoleName: "EnvironmentAdmin", CRNPattern: orgCRN + "/environment=env-1"}
	topicBinding := RoleBinding{ID: "rb-topic", Principal: "User:sa-1", RoleName: "DeveloperRead",
		CRNPattern: orgCRN + "/environment=env-1/cloud-cluster=lkc-1/kafka=lkc-1/topic=t"}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pat := r.URL.Query().Get("crn_pattern")
		var data []RoleBinding
		switch {
		case pat == orgCRN+"/*":
			http.Error(w, `{"errors":[{"detail":"wildcard not supported"}]}`, http.StatusBadRequest)
			return
		case pat == orgCRN:
			data = []RoleBinding{orgBinding}
		case pat == orgCRN+"/environment=env-1":
			data = []RoleBinding{envBinding}
		case strings.HasSuffix(pat, "/environment=env-1/*"):
			data = []RoleBinding{envBinding, topicBinding} // envBinding duplicated on purpose
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{}, "data": data})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", "s")
	got, err := fetchRoleBindings(context.Background(), c, "org-1",
		[]Environment{{ID: "env-1", DisplayName: "prod"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d bindings, want 3 (deduplicated): %+v", len(got), got)
	}
	ids := map[string]bool{}
	for _, b := range got {
		ids[b.ID] = true
	}
	for _, want := range []string{"rb-org", "rb-env", "rb-topic"} {
		if !ids[want] {
			t.Errorf("missing binding %s", want)
		}
	}
}
