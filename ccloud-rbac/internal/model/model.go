// Package model joins the raw API snapshot into an enriched, denormalized
// report model: principals resolved to display names, CRN patterns parsed
// into scope/resource, and roles mapped to read/write/admin access levels.
package model

import (
	"sort"
	"strings"

	"github.com/mk/ccloud-rbac/internal/ccloud"
)

// CRNInfo is the parsed form of a role binding crn_pattern.
type CRNInfo struct {
	OrgID        string
	EnvID        string
	ClusterID    string
	ScopeType    string // organization | environment | cluster | resource
	ResourceType string // organization, environment, cloud cluster, topic, consumer group, subject, ...
	ResourceName string // last segment value, may contain a trailing * wildcard
}

// resourceTypeNames maps CRN segment keys to human-friendly resource types.
var resourceTypeNames = map[string]string{
	"organization":     "organization",
	"environment":      "environment",
	"cloud-cluster":    "cloud cluster",
	"kafka":            "kafka cluster",
	"topic":            "topic",
	"group":            "consumer group",
	"transactional-id": "transactional ID",
	"schema-registry":  "schema registry",
	"subject":          "subject",
	"ksql":             "ksqlDB cluster",
	"connector":        "connector",
	"flink-region":     "flink region",
	"compute-pool":     "compute pool",
}

// ParseCRN parses a crn_pattern like
//
//	crn://confluent.cloud/organization=X/environment=Y/cloud-cluster=Z/kafka=Z/topic=orders-*
//
// into its scope identifiers and the leaf resource it targets.
func ParseCRN(crn string) CRNInfo {
	info := CRNInfo{}
	path := strings.TrimPrefix(crn, "crn://confluent.cloud/")

	lastKey, lastVal := "", ""
	for _, seg := range strings.Split(path, "/") {
		key, val, ok := strings.Cut(seg, "=")
		if !ok {
			continue
		}
		switch key {
		case "organization":
			info.OrgID = val
		case "environment":
			info.EnvID = val
		case "cloud-cluster":
			info.ClusterID = val
		}
		lastKey, lastVal = key, val
	}

	info.ResourceName = lastVal
	if name, ok := resourceTypeNames[lastKey]; ok {
		info.ResourceType = name
	} else {
		info.ResourceType = lastKey
	}

	switch lastKey {
	case "organization":
		info.ScopeType = "organization"
	case "environment":
		info.ScopeType = "environment"
	case "cloud-cluster", "kafka":
		info.ScopeType = "cluster"
	default:
		info.ScopeType = "resource"
	}
	return info
}

// accessLevels maps each predefined Confluent Cloud RBAC role to a coarse
// access level used for filtering and badges in the report.
var accessLevels = map[string]string{
	// Full control over their scope.
	"OrganizationAdmin": "admin",
	"EnvironmentAdmin":  "admin",
	"CloudClusterAdmin": "admin",
	"ResourceOwner":     "admin",
	"KsqlAdmin":         "admin",
	"NetworkAdmin":      "admin",
	"FlinkAdmin":        "admin",
	"AccountAdmin":      "admin",
	"BillingAdmin":      "admin",
	// Can produce / modify data or metadata.
	"DeveloperWrite":  "write",
	"DeveloperManage": "write",
	"DataSteward":     "write",
	"FlinkDeveloper":  "write",
	"Accountant":      "write",
	// Read-only visibility.
	"DeveloperRead": "read",
	"MetricsViewer": "read",
	"Operator":      "read",
	"DataDiscovery": "read",
}

// accessRank orders access levels for max-access aggregation.
var accessRank = map[string]int{"none": 0, "other": 1, "read": 2, "write": 3, "admin": 4}

// AccessForRole returns the coarse access level (admin/write/read) of a
// role, or "other" for roles not in the predefined map.
func AccessForRole(role string) string {
	if lvl, ok := accessLevels[role]; ok {
		return lvl
	}
	return "other"
}

// ParsePrincipal splits a role binding principal like "User:sa-abc123"
// into the resource ID and a principal type derived from its ID prefix.
func ParsePrincipal(principal string) (id, typ string) {
	id = principal
	if _, rest, ok := strings.Cut(principal, ":"); ok {
		id = rest
	}
	switch {
	case strings.HasPrefix(id, "u-"):
		typ = "user"
	case strings.HasPrefix(id, "sa-"):
		typ = "service-account"
	case strings.HasPrefix(id, "pool-"):
		typ = "identity-pool"
	case strings.HasPrefix(id, "group-"):
		typ = "sso-group"
	default:
		typ = "unknown"
	}
	return id, typ
}

// Binding is one role binding enriched with resolved names and parsed scope.
type Binding struct {
	ID            string `json:"id"`
	PrincipalID   string `json:"principalId"`
	PrincipalName string `json:"principalName"`
	PrincipalType string `json:"principalType"`
	Role          string `json:"role"`
	Access        string `json:"access"`
	ScopeType     string `json:"scopeType"`
	EnvID         string `json:"envId,omitempty"`
	EnvName       string `json:"envName,omitempty"`
	ClusterID     string `json:"clusterId,omitempty"`
	ClusterName   string `json:"clusterName,omitempty"`
	ResourceType  string `json:"resourceType"`
	ResourceName  string `json:"resourceName"`
	CRN           string `json:"crn"`
}

// Principal is one identity (user, service account, identity pool, ...)
// with aggregated access info.
type Principal struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Detail       string `json:"detail,omitempty"` // email, description or provider name
	BindingCount int    `json:"bindingCount"`
	MaxAccess    string `json:"maxAccess"`
}

// Pool is an identity pool nested under its provider in the report.
type Pool struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Description   string `json:"description,omitempty"`
	IdentityClaim string `json:"identityClaim"`
	Filter        string `json:"filter"`
	BindingCount  int    `json:"bindingCount"`
}

// Provider is an identity provider with its pools.
type Provider struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Issuer      string `json:"issuer"`
	JwksURI     string `json:"jwksUri"`
	Pools       []Pool `json:"pools"`
}

// EnvSummary names an environment and counts its clusters.
type EnvSummary struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ClusterCount int    `json:"clusterCount"`
}

// ClusterSummary names a cluster and its location.
type ClusterSummary struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	EnvID   string `json:"envId"`
	EnvName string `json:"envName"`
	Cloud   string `json:"cloud"`
	Region  string `json:"region"`
}

// Report is the full denormalized model embedded into the HTML report.
type Report struct {
	GeneratedAt  string           `json:"generatedAt"`
	OrgID        string           `json:"orgId"`
	OrgName      string           `json:"orgName"`
	Providers    []Provider       `json:"providers"`
	Principals   []Principal      `json:"principals"`
	Environments []EnvSummary     `json:"environments"`
	Clusters     []ClusterSummary `json:"clusters"`
	Bindings     []Binding        `json:"bindings"`
}

// Build joins a raw snapshot into the report model.
func Build(snap *ccloud.Snapshot, generatedAt string) *Report {
	envNames := map[string]string{}
	for _, e := range snap.Environments {
		envNames[e.ID] = e.DisplayName
	}
	clusterNames := map[string]string{}
	for _, c := range snap.Clusters {
		clusterNames[c.ID] = c.Spec.DisplayName
	}
	providerNames := map[string]string{}
	for _, p := range snap.Providers {
		providerNames[p.ID] = p.DisplayName
	}

	// Seed principals from every known identity so identities without any
	// role binding still show up in the report.
	principals := map[string]*Principal{}
	for _, u := range snap.Users {
		name := u.FullName
		if name == "" {
			name = u.Email
		}
		principals[u.ID] = &Principal{ID: u.ID, Name: name, Type: "user", Detail: u.Email, MaxAccess: "none"}
	}
	for _, sa := range snap.ServiceAccounts {
		principals[sa.ID] = &Principal{ID: sa.ID, Name: sa.DisplayName, Type: "service-account", Detail: sa.Description, MaxAccess: "none"}
	}
	for _, p := range snap.Pools {
		principals[p.ID] = &Principal{ID: p.ID, Name: p.DisplayName, Type: "identity-pool",
			Detail: "via " + providerNames[p.ProviderID], MaxAccess: "none"}
	}

	poolBindings := map[string]int{}
	bindings := make([]Binding, 0, len(snap.RoleBindings))
	for _, rb := range snap.RoleBindings {
		id, typ := ParsePrincipal(rb.Principal)
		info := ParseCRN(rb.CRNPattern)
		access := AccessForRole(rb.RoleName)

		p, ok := principals[id]
		if !ok {
			// Binding references an identity we could not list (deleted, or
			// from an endpoint we do not fetch, e.g. SSO group mappings).
			p = &Principal{ID: id, Name: id, Type: typ, MaxAccess: "none"}
			principals[id] = p
		}
		p.BindingCount++
		if accessRank[access] > accessRank[p.MaxAccess] {
			p.MaxAccess = access
		}
		if typ == "identity-pool" {
			poolBindings[id]++
		}

		bindings = append(bindings, Binding{
			ID:            rb.ID,
			PrincipalID:   id,
			PrincipalName: p.Name,
			PrincipalType: p.Type,
			Role:          rb.RoleName,
			Access:        access,
			ScopeType:     info.ScopeType,
			EnvID:         info.EnvID,
			EnvName:       envNames[info.EnvID],
			ClusterID:     info.ClusterID,
			ClusterName:   clusterNames[info.ClusterID],
			ResourceType:  info.ResourceType,
			ResourceName:  info.ResourceName,
			CRN:           rb.CRNPattern,
		})
	}

	report := &Report{
		GeneratedAt: generatedAt,
		OrgID:       snap.Organization.ID,
		OrgName:     snap.Organization.DisplayName,
		Bindings:    bindings,
	}

	for _, p := range snap.Providers {
		pv := Provider{ID: p.ID, Name: p.DisplayName, Description: p.Description,
			Issuer: p.Issuer, JwksURI: p.JwksURI, Pools: []Pool{}}
		for _, pool := range snap.Pools {
			if pool.ProviderID == p.ID {
				pv.Pools = append(pv.Pools, Pool{
					ID: pool.ID, Name: pool.DisplayName, Description: pool.Description,
					IdentityClaim: pool.IdentityClaim, Filter: pool.Filter,
					BindingCount: poolBindings[pool.ID],
				})
			}
		}
		report.Providers = append(report.Providers, pv)
	}

	clustersPerEnv := map[string]int{}
	for _, c := range snap.Clusters {
		clustersPerEnv[c.Spec.Environment.ID]++
		report.Clusters = append(report.Clusters, ClusterSummary{
			ID: c.ID, Name: c.Spec.DisplayName,
			EnvID: c.Spec.Environment.ID, EnvName: envNames[c.Spec.Environment.ID],
			Cloud: c.Spec.Cloud, Region: c.Spec.Region,
		})
	}
	for _, e := range snap.Environments {
		report.Environments = append(report.Environments, EnvSummary{
			ID: e.ID, Name: e.DisplayName, ClusterCount: clustersPerEnv[e.ID],
		})
	}

	for _, p := range principals {
		report.Principals = append(report.Principals, *p)
	}
	sort.Slice(report.Principals, func(i, j int) bool {
		a, b := report.Principals[i], report.Principals[j]
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		return a.Name < b.Name
	})

	return report
}
