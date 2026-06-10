// Package ccloud provides a minimal read-only client for the Confluent
// Cloud APIs needed to build an RBAC report: IAM v2 (identity providers,
// identity pools, role bindings, users, service accounts), org/v2
// (organizations, environments) and cmk/v2 (Kafka clusters).
package ccloud

// ListMeta carries pagination links returned by every list endpoint.
// Next is a full URL containing the page_token for the next page; it is
// empty on the last page.
type ListMeta struct {
	Next      string `json:"next,omitempty"`
	TotalSize int    `json:"total_size,omitempty"`
}

// listEnvelope is the common wrapper of Confluent Cloud list responses.
type listEnvelope[T any] struct {
	Metadata ListMeta `json:"metadata"`
	Data     []T      `json:"data"`
}

// IdentityProvider is an OAuth/OIDC provider registered with the
// organization (iam/v2/identity-providers).
type IdentityProvider struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	Issuer      string `json:"issuer"`
	JwksURI     string `json:"jwks_uri"`
	State       string `json:"state,omitempty"`
}

// IdentityPool groups external identities of one provider via a
// claims-based filter (iam/v2/identity-providers/{id}/identity-pools).
// Its principal in role bindings is "User:<ID>".
type IdentityPool struct {
	ID            string `json:"id"`
	DisplayName   string `json:"display_name"`
	Description   string `json:"description"`
	IdentityClaim string `json:"identity_claim"`
	Filter        string `json:"filter"`
	State         string `json:"state,omitempty"`
	// ProviderID is filled in by the fetcher; the API nests pools under
	// their provider rather than embedding a reference.
	ProviderID string `json:"provider_id,omitempty"`
}

// RoleBinding grants a role to a principal on resources matching a CRN
// pattern (iam/v2/role-bindings).
type RoleBinding struct {
	ID         string `json:"id"`
	Principal  string `json:"principal"`
	RoleName   string `json:"role_name"`
	CRNPattern string `json:"crn_pattern"`
}

// User is a human account (iam/v2/users).
type User struct {
	ID       string `json:"id"`
	Email    string `json:"email"`
	FullName string `json:"full_name"`
	AuthType string `json:"auth_type,omitempty"`
}

// ServiceAccount is a machine account (iam/v2/service-accounts).
type ServiceAccount struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
}

// Organization (org/v2/organizations); the CRN organization segment uses
// its resource ID.
type Organization struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

// Environment (org/v2/environments).
type Environment struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

// KafkaCluster (cmk/v2/clusters?environment=...).
type KafkaCluster struct {
	ID   string `json:"id"`
	Spec struct {
		DisplayName string `json:"display_name"`
		Cloud       string `json:"cloud"`
		Region      string `json:"region"`
		Environment struct {
			ID string `json:"id"`
		} `json:"environment"`
	} `json:"spec"`
}

// Snapshot bundles everything fetched from the APIs in one struct.
type Snapshot struct {
	Organization    Organization       `json:"organization"`
	Providers       []IdentityProvider `json:"identity_providers"`
	Pools           []IdentityPool     `json:"identity_pools"`
	RoleBindings    []RoleBinding      `json:"role_bindings"`
	Users           []User             `json:"users"`
	ServiceAccounts []ServiceAccount   `json:"service_accounts"`
	Environments    []Environment      `json:"environments"`
	Clusters        []KafkaCluster     `json:"clusters"`
}
