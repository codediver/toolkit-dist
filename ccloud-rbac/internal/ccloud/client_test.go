package ccloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

// TestListAllPagination verifies that listAll follows metadata.next links
// across pages and rewrites next-link hosts to the configured base URL.
func TestListAllPagination(t *testing.T) {
	items := make([]User, 7)
	for i := range items {
		items[i] = User{ID: fmt.Sprintf("u-%d", i), Email: fmt.Sprintf("u%d@x", i)}
	}

	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if user, pass, ok := r.BasicAuth(); !ok || user != "key" || pass != "secret" {
			t.Errorf("missing/wrong basic auth: %q %q", user, pass)
		}
		start, _ := strconv.Atoi(r.URL.Query().Get("page_token"))
		end := min(start+3, len(items))
		next := ""
		if end < len(items) {
			// Deliberately use a foreign host to exercise host rewriting.
			next = "https://api.confluent.cloud" + r.URL.Path + "?page_size=3&page_token=" + strconv.Itoa(end)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"next": next},
			"data":     items[start:end],
		})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "key", "secret")
	got, err := listAll[User](context.Background(), c, "/iam/v2/users", url.Values{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(items) {
		t.Fatalf("got %d users, want %d", len(got), len(items))
	}
	if requests != 3 {
		t.Errorf("got %d requests, want 3 pages", requests)
	}
	if got[6].ID != "u-6" {
		t.Errorf("last item = %+v", got[6])
	}
}

func TestListAllAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"errors":[{"detail":"crn_pattern is required"}]}`, http.StatusBadRequest)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "k", "s")
	_, err := listAll[RoleBinding](context.Background(), c, "/iam/v2/role-bindings", nil)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !IsNotAllowed(err) {
		t.Errorf("IsNotAllowed(%v) = false, want true", err)
	}
	// Must also match through wrapping.
	if !IsNotAllowed(fmt.Errorf("outer: %w", err)) {
		t.Errorf("IsNotAllowed should unwrap wrapped errors")
	}
}
