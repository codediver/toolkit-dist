package ccloud

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
)

// FetchAll retrieves every resource needed for the RBAC report. It walks:
//
//	org/v2/organizations            -> organization id (CRN root)
//	org/v2/environments             -> environment names
//	cmk/v2/clusters?environment=X   -> cluster names, per environment
//	iam/v2/users                    -> user principals
//	iam/v2/service-accounts         -> service account principals
//	iam/v2/identity-providers       -> providers
//	  /{id}/identity-pools          -> pools (principals User:pool-...)
//	iam/v2/role-bindings            -> bindings, scoped by crn_pattern
func FetchAll(ctx context.Context, c *Client) (*Snapshot, error) {
	snap := &Snapshot{}

	orgs, err := listAll[Organization](ctx, c, "/org/v2/organizations", nil)
	if err != nil {
		return nil, fmt.Errorf("list organizations: %w", err)
	}
	if len(orgs) == 0 {
		return nil, fmt.Errorf("no organization visible to this API key")
	}
	snap.Organization = orgs[0]

	snap.Environments, err = listAll[Environment](ctx, c, "/org/v2/environments", nil)
	if err != nil {
		return nil, fmt.Errorf("list environments: %w", err)
	}

	for _, env := range snap.Environments {
		clusters, err := listAll[KafkaCluster](ctx, c, "/cmk/v2/clusters",
			url.Values{"environment": {env.ID}})
		if err != nil {
			return nil, fmt.Errorf("list clusters in %s: %w", env.ID, err)
		}
		snap.Clusters = append(snap.Clusters, clusters...)
	}

	snap.Users, err = listAll[User](ctx, c, "/iam/v2/users", nil)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}

	snap.ServiceAccounts, err = listAll[ServiceAccount](ctx, c, "/iam/v2/service-accounts", nil)
	if err != nil {
		return nil, fmt.Errorf("list service accounts: %w", err)
	}

	snap.Providers, err = listAll[IdentityProvider](ctx, c, "/iam/v2/identity-providers", nil)
	if err != nil {
		return nil, fmt.Errorf("list identity providers: %w", err)
	}

	for _, p := range snap.Providers {
		pools, err := listAll[IdentityPool](ctx, c,
			"/iam/v2/identity-providers/"+url.PathEscape(p.ID)+"/identity-pools", nil)
		if err != nil {
			return nil, fmt.Errorf("list identity pools of %s: %w", p.ID, err)
		}
		for i := range pools {
			pools[i].ProviderID = p.ID
		}
		snap.Pools = append(snap.Pools, pools...)
	}

	snap.RoleBindings, err = fetchRoleBindings(ctx, c, snap.Organization.ID, snap.Environments)
	if err != nil {
		return nil, fmt.Errorf("list role bindings: %w", err)
	}

	return snap, nil
}

// fetchRoleBindings lists role bindings at every scope. The list endpoint
// requires a crn_pattern filter; an org-level wildcard usually returns
// everything, but if the API rejects it we fall back to querying each
// environment subtree explicitly. Results are de-duplicated by binding ID.
func fetchRoleBindings(ctx context.Context, c *Client, orgID string, envs []Environment) ([]RoleBinding, error) {
	orgCRN := "crn://confluent.cloud/organization=" + orgID

	patterns := []string{orgCRN, orgCRN + "/*"}
	seen := map[string]RoleBinding{}

	for _, pat := range patterns {
		bindings, err := listAll[RoleBinding](ctx, c, "/iam/v2/role-bindings",
			url.Values{"crn_pattern": {pat}})
		if err != nil {
			if IsNotAllowed(err) && pat != orgCRN {
				// Wildcard pattern rejected: fall back to per-environment queries.
				log.Printf("crn_pattern %q rejected, falling back to per-environment queries", pat)
				if err := fetchEnvRoleBindings(ctx, c, orgCRN, envs, seen); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		for _, b := range bindings {
			seen[b.ID] = b
		}
	}

	out := make([]RoleBinding, 0, len(seen))
	for _, b := range seen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func fetchEnvRoleBindings(ctx context.Context, c *Client, orgCRN string, envs []Environment, seen map[string]RoleBinding) error {
	for _, env := range envs {
		for _, pat := range []string{
			orgCRN + "/environment=" + env.ID,
			orgCRN + "/environment=" + env.ID + "/*",
		} {
			bindings, err := listAll[RoleBinding](ctx, c, "/iam/v2/role-bindings",
				url.Values{"crn_pattern": {pat}})
			if err != nil {
				if IsNotAllowed(err) && strings.HasSuffix(pat, "/*") {
					// Environment wildcard rejected too: keep what the exact
					// patterns returned instead of failing the whole fetch,
					// but make the gap visible.
					log.Printf("warning: crn_pattern %q rejected; bindings below environment scope in %s may be missing", pat, env.ID)
					continue
				}
				return fmt.Errorf("crn_pattern %q: %w", pat, err)
			}
			for _, b := range bindings {
				seen[b.ID] = b
			}
		}
	}
	return nil
}
