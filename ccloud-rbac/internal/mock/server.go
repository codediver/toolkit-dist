package mock

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"

	"github.com/mk/ccloud-rbac/internal/ccloud"
)

// NewServer starts an in-process HTTP server that mimics the Confluent
// Cloud list endpoints over the canned Data(). It honors page_size /
// page_token pagination (so the real client pagination code is exercised)
// and crn_pattern prefix filtering on role bindings. The caller must Close
// the returned server.
func NewServer() *httptest.Server {
	snap := Data()
	mux := http.NewServeMux()

	mux.HandleFunc("/org/v2/organizations", func(w http.ResponseWriter, r *http.Request) {
		writePage(w, r, []ccloud.Organization{snap.Organization})
	})
	mux.HandleFunc("/org/v2/environments", func(w http.ResponseWriter, r *http.Request) {
		writePage(w, r, snap.Environments)
	})
	mux.HandleFunc("/cmk/v2/clusters", func(w http.ResponseWriter, r *http.Request) {
		env := r.URL.Query().Get("environment")
		var out []ccloud.KafkaCluster
		for _, c := range snap.Clusters {
			if env == "" || c.Spec.Environment.ID == env {
				out = append(out, c)
			}
		}
		writePage(w, r, out)
	})
	mux.HandleFunc("/iam/v2/users", func(w http.ResponseWriter, r *http.Request) {
		writePage(w, r, snap.Users)
	})
	mux.HandleFunc("/iam/v2/service-accounts", func(w http.ResponseWriter, r *http.Request) {
		writePage(w, r, snap.ServiceAccounts)
	})
	mux.HandleFunc("/iam/v2/identity-providers", func(w http.ResponseWriter, r *http.Request) {
		writePage(w, r, snap.Providers)
	})
	mux.HandleFunc("/iam/v2/identity-providers/", func(w http.ResponseWriter, r *http.Request) {
		// /iam/v2/identity-providers/{id}/identity-pools
		rest := strings.TrimPrefix(r.URL.Path, "/iam/v2/identity-providers/")
		parts := strings.Split(rest, "/")
		if len(parts) != 2 || parts[1] != "identity-pools" {
			http.NotFound(w, r)
			return
		}
		var out []ccloud.IdentityPool
		for _, p := range snap.Pools {
			if p.ProviderID == parts[0] {
				// The real API does not return a provider reference on the
				// pool object; strip it so the client has to fill it in.
				p.ProviderID = ""
				out = append(out, p)
			}
		}
		writePage(w, r, out)
	})
	mux.HandleFunc("/iam/v2/role-bindings", func(w http.ResponseWriter, r *http.Request) {
		pattern := r.URL.Query().Get("crn_pattern")
		if pattern == "" {
			http.Error(w, `{"errors":[{"detail":"crn_pattern is required"}]}`, http.StatusBadRequest)
			return
		}
		var out []ccloud.RoleBinding
		for _, b := range snap.RoleBindings {
			if matchCRN(pattern, b.CRNPattern) {
				out = append(out, b)
			}
		}
		writePage(w, r, out)
	})

	return httptest.NewServer(mux)
}

// matchCRN reports whether a binding CRN falls under the queried pattern.
// "crn://.../organization=x" matches only org-scope bindings, while a
// trailing "/*" matches the whole subtree — mirroring API semantics.
func matchCRN(pattern, crn string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "/*"); ok {
		return crn == prefix || strings.HasPrefix(crn, prefix+"/")
	}
	return crn == pattern
}

// writePage serializes one page of items using page_size / page_token, with
// a metadata.next link when more items remain — the same envelope shape the
// real API returns.
func writePage[T any](w http.ResponseWriter, r *http.Request, items []T) {
	q := r.URL.Query()
	size := 100
	if s, err := strconv.Atoi(q.Get("page_size")); err == nil && s > 0 {
		size = s
	}
	start := 0
	if t, err := strconv.Atoi(q.Get("page_token")); err == nil && t >= 0 {
		start = t
	}
	end := min(start+size, len(items))
	if start > end {
		start = end
	}

	next := ""
	if end < len(items) {
		nq := r.URL.Query()
		nq.Set("page_token", strconv.Itoa(end))
		next = "http://" + r.Host + r.URL.Path + "?" + nq.Encode()
	}

	resp := map[string]any{
		"metadata": map[string]any{"next": next, "total_size": len(items)},
		"data":     items[start:end],
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		fmt.Fprintln(os.Stderr, "mock server: encode response:", err)
	}
}
