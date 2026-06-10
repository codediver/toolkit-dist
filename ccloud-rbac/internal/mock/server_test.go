package mock

import (
	"context"
	"testing"

	"github.com/mk/ccloud-rbac/internal/ccloud"
)

// TestFetchAllAgainstMock runs the real fetch pipeline against the mock
// server and checks the snapshot matches the canned data.
func TestFetchAllAgainstMock(t *testing.T) {
	srv := NewServer()
	defer srv.Close()

	c := ccloud.NewClient(srv.URL, "k", "s")
	snap, err := ccloud.FetchAll(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	want := Data()
	if snap.Organization.ID != want.Organization.ID {
		t.Errorf("org = %q, want %q", snap.Organization.ID, want.Organization.ID)
	}
	checks := []struct {
		name      string
		got, want int
	}{
		{"environments", len(snap.Environments), len(want.Environments)},
		{"clusters", len(snap.Clusters), len(want.Clusters)},
		{"users", len(snap.Users), len(want.Users)},
		{"service accounts", len(snap.ServiceAccounts), len(want.ServiceAccounts)},
		{"providers", len(snap.Providers), len(want.Providers)},
		{"pools", len(snap.Pools), len(want.Pools)},
		{"role bindings", len(snap.RoleBindings), len(want.RoleBindings)},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s: got %d, want %d", ch.name, ch.got, ch.want)
		}
	}

	// The mock API strips provider references from pools; the fetcher must
	// reattach them from the URL it fetched each pool through.
	for _, p := range snap.Pools {
		if p.ProviderID == "" {
			t.Errorf("pool %s has no provider ID", p.ID)
		}
	}
}

// TestMatchCRN pins the subtree-matching semantics of the mock server.
func TestMatchCRN(t *testing.T) {
	org := "crn://confluent.cloud/organization=x"
	tests := []struct {
		pattern, crn string
		want         bool
	}{
		{org, org, true},
		{org, org + "/environment=e", false},
		{org + "/*", org, true},
		{org + "/*", org + "/environment=e/cloud-cluster=c", true},
		{org + "/environment=e/*", org + "/environment=e2", false},
		{org + "/environment=e/*", org + "/environment=e/cloud-cluster=c", true},
	}
	for _, tt := range tests {
		if got := matchCRN(tt.pattern, tt.crn); got != tt.want {
			t.Errorf("matchCRN(%q, %q) = %v, want %v", tt.pattern, tt.crn, got, tt.want)
		}
	}
}
