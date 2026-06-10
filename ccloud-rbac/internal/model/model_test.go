package model

import (
	"testing"

	"github.com/mk/ccloud-rbac/internal/ccloud"
)

func TestParseCRN(t *testing.T) {
	const base = "crn://confluent.cloud/organization=org-1"
	tests := []struct {
		name string
		crn  string
		want CRNInfo
	}{
		{
			name: "organization scope",
			crn:  base,
			want: CRNInfo{OrgID: "org-1", ScopeType: "organization", ResourceType: "organization", ResourceName: "org-1"},
		},
		{
			name: "environment scope",
			crn:  base + "/environment=env-1",
			want: CRNInfo{OrgID: "org-1", EnvID: "env-1", ScopeType: "environment", ResourceType: "environment", ResourceName: "env-1"},
		},
		{
			name: "cluster scope",
			crn:  base + "/environment=env-1/cloud-cluster=lkc-1",
			want: CRNInfo{OrgID: "org-1", EnvID: "env-1", ClusterID: "lkc-1", ScopeType: "cluster", ResourceType: "cloud cluster", ResourceName: "lkc-1"},
		},
		{
			name: "topic prefix pattern",
			crn:  base + "/environment=env-1/cloud-cluster=lkc-1/kafka=lkc-1/topic=orders-*",
			want: CRNInfo{OrgID: "org-1", EnvID: "env-1", ClusterID: "lkc-1", ScopeType: "resource", ResourceType: "topic", ResourceName: "orders-*"},
		},
		{
			name: "consumer group",
			crn:  base + "/environment=env-1/cloud-cluster=lkc-1/kafka=lkc-1/group=cg-1",
			want: CRNInfo{OrgID: "org-1", EnvID: "env-1", ClusterID: "lkc-1", ScopeType: "resource", ResourceType: "consumer group", ResourceName: "cg-1"},
		},
		{
			name: "schema registry subject",
			crn:  base + "/environment=env-1/schema-registry=lsrc-1/subject=orders-value",
			want: CRNInfo{OrgID: "org-1", EnvID: "env-1", ScopeType: "resource", ResourceType: "subject", ResourceName: "orders-value"},
		},
		{
			name: "transactional id",
			crn:  base + "/environment=env-1/cloud-cluster=lkc-1/kafka=lkc-1/transactional-id=tx-*",
			want: CRNInfo{OrgID: "org-1", EnvID: "env-1", ClusterID: "lkc-1", ScopeType: "resource", ResourceType: "transactional ID", ResourceName: "tx-*"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseCRN(tt.crn)
			if got != tt.want {
				t.Errorf("ParseCRN(%q)\n got %+v\nwant %+v", tt.crn, got, tt.want)
			}
		})
	}
}

func TestAccessForRole(t *testing.T) {
	tests := []struct {
		role string
		want string
	}{
		{"OrganizationAdmin", "admin"},
		{"EnvironmentAdmin", "admin"},
		{"CloudClusterAdmin", "admin"},
		{"ResourceOwner", "admin"},
		{"DeveloperWrite", "write"},
		{"DeveloperManage", "write"},
		{"DataSteward", "write"},
		{"FlinkDeveloper", "write"},
		{"DeveloperRead", "read"},
		{"MetricsViewer", "read"},
		{"Operator", "read"},
		{"DataDiscovery", "read"},
		{"SomeFutureRole", "other"},
	}
	for _, tt := range tests {
		if got := AccessForRole(tt.role); got != tt.want {
			t.Errorf("AccessForRole(%q) = %q, want %q", tt.role, got, tt.want)
		}
	}
}

func TestParsePrincipal(t *testing.T) {
	tests := []struct {
		principal string
		wantID    string
		wantType  string
	}{
		{"User:u-abc123", "u-abc123", "user"},
		{"User:sa-abc123", "sa-abc123", "service-account"},
		{"User:pool-abc12", "pool-abc12", "identity-pool"},
		{"User:group-ab12", "group-ab12", "sso-group"},
		{"User:something", "something", "unknown"},
	}
	for _, tt := range tests {
		id, typ := ParsePrincipal(tt.principal)
		if id != tt.wantID || typ != tt.wantType {
			t.Errorf("ParsePrincipal(%q) = (%q, %q), want (%q, %q)",
				tt.principal, id, typ, tt.wantID, tt.wantType)
		}
	}
}

func TestBuild(t *testing.T) {
	snap := &ccloud.Snapshot{
		Organization: ccloud.Organization{ID: "org-1", DisplayName: "Test Org"},
		Environments: []ccloud.Environment{{ID: "env-1", DisplayName: "prod"}},
		Users:        []ccloud.User{{ID: "u-1", Email: "a@b.c", FullName: "Alice"}},
		ServiceAccounts: []ccloud.ServiceAccount{
			{ID: "sa-1", DisplayName: "svc", Description: "a service"},
		},
		Providers: []ccloud.IdentityProvider{{ID: "op-1", DisplayName: "Okta"}},
		Pools: []ccloud.IdentityPool{
			{ID: "pool-1", ProviderID: "op-1", DisplayName: "team-pool"},
		},
		RoleBindings: []ccloud.RoleBinding{
			{ID: "rb-1", Principal: "User:u-1", RoleName: "OrganizationAdmin",
				CRNPattern: "crn://confluent.cloud/organization=org-1"},
			{ID: "rb-2", Principal: "User:pool-1", RoleName: "DeveloperRead",
				CRNPattern: "crn://confluent.cloud/organization=org-1/environment=env-1"},
			{ID: "rb-3", Principal: "User:u-gone", RoleName: "DeveloperWrite",
				CRNPattern: "crn://confluent.cloud/organization=org-1/environment=env-1"},
		},
	}

	r := Build(snap, "2026-06-09T00:00:00Z")

	if r.OrgName != "Test Org" || len(r.Bindings) != 3 {
		t.Fatalf("unexpected report: org=%q bindings=%d", r.OrgName, len(r.Bindings))
	}

	byID := map[string]Binding{}
	for _, b := range r.Bindings {
		byID[b.ID] = b
	}

	if b := byID["rb-1"]; b.PrincipalName != "Alice" || b.Access != "admin" || b.PrincipalType != "user" {
		t.Errorf("rb-1 enrichment wrong: %+v", b)
	}
	if b := byID["rb-2"]; b.PrincipalName != "team-pool" || b.EnvName != "prod" || b.PrincipalType != "identity-pool" {
		t.Errorf("rb-2 enrichment wrong: %+v", b)
	}
	// Principal not present in the snapshot must still render, by ID.
	if b := byID["rb-3"]; b.PrincipalName != "u-gone" {
		t.Errorf("rb-3 should fall back to principal ID as name: %+v", b)
	}

	// Principals include all known identities plus the orphaned one.
	wantPrincipals := 4 // u-1, sa-1 (0 bindings), pool-1, u-gone
	if len(r.Principals) != wantPrincipals {
		t.Errorf("got %d principals, want %d: %+v", len(r.Principals), wantPrincipals, r.Principals)
	}
	for _, p := range r.Principals {
		if p.ID == "u-1" && p.MaxAccess != "admin" {
			t.Errorf("u-1 MaxAccess = %q, want admin", p.MaxAccess)
		}
		if p.ID == "sa-1" && (p.BindingCount != 0 || p.MaxAccess != "none") {
			t.Errorf("sa-1 should have no bindings: %+v", p)
		}
	}

	if len(r.Providers) != 1 || len(r.Providers[0].Pools) != 1 {
		t.Fatalf("provider/pool nesting wrong: %+v", r.Providers)
	}
	if r.Providers[0].Pools[0].BindingCount != 1 {
		t.Errorf("pool binding count = %d, want 1", r.Providers[0].Pools[0].BindingCount)
	}
}
