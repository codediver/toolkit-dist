// Package mock provides realistic sample data and an in-process HTTP
// server that mimics the Confluent Cloud list endpoints, so the CLI can be
// exercised end-to-end without real credentials.
package mock

import (
	"fmt"

	"github.com/mk/ccloud-rbac/internal/ccloud"
)

// OrgID is the mock organization resource ID used in all CRN patterns.
const OrgID = "a1b2c3d4-0000-4000-8000-0000acme0001"

const orgCRN = "crn://confluent.cloud/organization=" + OrgID

// Data returns the canned snapshot served by the mock API. It models a
// small company ("Acme Corp") with dev/staging/prod environments, two
// OIDC identity providers (Okta for humans-adjacent workloads, Azure AD
// for CI/CD), and a spread of role bindings at org, environment, cluster
// and resource (topic / consumer group / subject) scope.
func Data() *ccloud.Snapshot {
	snap := &ccloud.Snapshot{
		Organization: ccloud.Organization{ID: OrgID, DisplayName: "Acme Corp"},

		Environments: []ccloud.Environment{
			{ID: "env-dev001", DisplayName: "dev"},
			{ID: "env-stg001", DisplayName: "staging"},
			{ID: "env-prd001", DisplayName: "production"},
		},

		Users: []ccloud.User{
			{ID: "u-alice01", Email: "alice@acme.example", FullName: "Alice Anders", AuthType: "AUTH_TYPE_SSO"},
			{ID: "u-bob0001", Email: "bob@acme.example", FullName: "Bob Baker", AuthType: "AUTH_TYPE_SSO"},
			{ID: "u-carol01", Email: "carol@acme.example", FullName: "Carol Chen", AuthType: "AUTH_TYPE_SSO"},
			{ID: "u-dave001", Email: "dave@acme.example", FullName: "Dave Diaz", AuthType: "AUTH_TYPE_LOCAL"},
			{ID: "u-erin001", Email: "erin@acme.example", FullName: "Erin Engel", AuthType: "AUTH_TYPE_SSO"},
			{ID: "u-frank01", Email: "frank@acme.example", FullName: "Frank Fox", AuthType: "AUTH_TYPE_LOCAL"},
			{ID: "u-grace01", Email: "grace@acme.example", FullName: "Grace Gupta", AuthType: "AUTH_TYPE_SSO"},
			{ID: "u-heidi01", Email: "heidi@acme.example", FullName: "Heidi Huang", AuthType: "AUTH_TYPE_SSO"},
		},

		ServiceAccounts: []ccloud.ServiceAccount{
			{ID: "sa-orders1", DisplayName: "orders-service", Description: "Order processing microservice"},
			{ID: "sa-paymnt1", DisplayName: "payments-service", Description: "Payment processing microservice"},
			{ID: "sa-anlyt01", DisplayName: "analytics-readonly", Description: "Read-only analytics consumers"},
			{ID: "sa-connct1", DisplayName: "connect-cluster", Description: "Managed connectors runtime"},
			{ID: "sa-cicd001", DisplayName: "ci-pipeline", Description: "Terraform / CI automation"},
			{ID: "sa-monitr1", DisplayName: "monitoring-agent", Description: "Metrics scraper for Datadog"},
		},

		Providers: []ccloud.IdentityProvider{
			{
				ID: "op-okta01", DisplayName: "Okta Production",
				Description: "Workforce identity for data platform workloads",
				Issuer:      "https://acme.okta.com",
				JwksURI:     "https://acme.okta.com/oauth2/v1/keys",
			},
			{
				ID: "op-azure1", DisplayName: "Azure AD CI/CD",
				Description: "Entra ID tenant for build and deployment pipelines",
				Issuer:      "https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/v2.0",
				JwksURI:     "https://login.microsoftonline.com/11111111-2222-3333-4444-555555555555/discovery/v2.0/keys",
			},
		},

		Pools: []ccloud.IdentityPool{
			{
				ID: "pool-dteng1", ProviderID: "op-okta01", DisplayName: "data-engineering",
				Description:   "Data engineering team workloads",
				IdentityClaim: "claims.sub",
				Filter:        `claims.groups.exists(g, g == "data-engineering")`,
			},
			{
				ID: "pool-anlys1", ProviderID: "op-okta01", DisplayName: "analytics-team",
				Description:   "Analytics notebooks and dashboards",
				IdentityClaim: "claims.sub",
				Filter:        `claims.groups.exists(g, g == "analytics")`,
			},
			{
				ID: "pool-cicd01", ProviderID: "op-azure1", DisplayName: "cicd-prod-deployer",
				Description:   "Production deployment pipeline",
				IdentityClaim: "claims.azp",
				Filter:        `claims.appid == "9d9e9f00-prod-cicd-app"`,
			},
			{
				ID: "pool-flink1", ProviderID: "op-azure1", DisplayName: "flink-jobs",
				Description:   "Flink streaming jobs",
				IdentityClaim: "claims.azp",
				Filter:        `claims.appid == "7a7b7c00-flink-app"`,
			},
		},
	}

	snap.Clusters = []ccloud.KafkaCluster{
		cluster("lkc-dev001", "dev-sandbox", "AWS", "us-east-1", "env-dev001"),
		cluster("lkc-stg001", "staging-core", "AWS", "us-east-1", "env-stg001"),
		cluster("lkc-prd001", "prod-core", "AWS", "us-east-1", "env-prd001"),
		cluster("lkc-prd002", "prod-analytics", "GCP", "us-central1", "env-prd001"),
	}

	snap.RoleBindings = roleBindings()
	return snap
}

func cluster(id, name, cloud, region, envID string) ccloud.KafkaCluster {
	var c ccloud.KafkaCluster
	c.ID = id
	c.Spec.DisplayName = name
	c.Spec.Cloud = cloud
	c.Spec.Region = region
	c.Spec.Environment.ID = envID
	return c
}

func roleBindings() []ccloud.RoleBinding {
	prod := orgCRN + "/environment=env-prd001"
	stg := orgCRN + "/environment=env-stg001"
	dev := orgCRN + "/environment=env-dev001"
	prodCore := prod + "/cloud-cluster=lkc-prd001"
	prodAnalytics := prod + "/cloud-cluster=lkc-prd002"
	stgCore := stg + "/cloud-cluster=lkc-stg001"
	devSandbox := dev + "/cloud-cluster=lkc-dev001"

	rbs := []ccloud.RoleBinding{
		// --- organization scope ---
		{Principal: "User:u-alice01", RoleName: "OrganizationAdmin", CRNPattern: orgCRN},
		{Principal: "User:u-heidi01", RoleName: "Operator", CRNPattern: orgCRN},
		{Principal: "User:sa-monitr1", RoleName: "MetricsViewer", CRNPattern: orgCRN},
		{Principal: "User:u-frank01", RoleName: "MetricsViewer", CRNPattern: orgCRN},

		// --- environment scope ---
		{Principal: "User:u-bob0001", RoleName: "EnvironmentAdmin", CRNPattern: prod},
		{Principal: "User:sa-cicd001", RoleName: "EnvironmentAdmin", CRNPattern: stg},
		{Principal: "User:pool-cicd01", RoleName: "EnvironmentAdmin", CRNPattern: prod},
		{Principal: "User:u-dave001", RoleName: "EnvironmentAdmin", CRNPattern: dev},
		{Principal: "User:pool-dteng1", RoleName: "DeveloperManage", CRNPattern: stg},

		// --- cluster scope ---
		{Principal: "User:u-carol01", RoleName: "CloudClusterAdmin", CRNPattern: prodCore},
		{Principal: "User:u-carol01", RoleName: "CloudClusterAdmin", CRNPattern: prodAnalytics},
		{Principal: "User:sa-connct1", RoleName: "DeveloperManage", CRNPattern: stgCore},
		{Principal: "User:u-erin001", RoleName: "DeveloperRead", CRNPattern: devSandbox},

		// --- topic scope (literal and prefix patterns) ---
		{Principal: "User:sa-orders1", RoleName: "DeveloperWrite",
			CRNPattern: prodCore + "/kafka=lkc-prd001/topic=orders-*"},
		{Principal: "User:sa-orders1", RoleName: "DeveloperRead",
			CRNPattern: prodCore + "/kafka=lkc-prd001/topic=inventory.events"},
		{Principal: "User:sa-paymnt1", RoleName: "ResourceOwner",
			CRNPattern: prodCore + "/kafka=lkc-prd001/topic=payments.transactions"},
		{Principal: "User:u-grace01", RoleName: "ResourceOwner",
			CRNPattern: prodCore + "/kafka=lkc-prd001/topic=payments-*"},
		{Principal: "User:sa-anlyt01", RoleName: "DeveloperRead",
			CRNPattern: prodAnalytics + "/kafka=lkc-prd002/topic=*"},
		{Principal: "User:pool-anlys1", RoleName: "DeveloperRead",
			CRNPattern: prodAnalytics + "/kafka=lkc-prd002/topic=events-*"},
		{Principal: "User:pool-flink1", RoleName: "DeveloperWrite",
			CRNPattern: prodAnalytics + "/kafka=lkc-prd002/topic=flink.derived-*"},
		{Principal: "User:u-dave001", RoleName: "DeveloperWrite",
			CRNPattern: stgCore + "/kafka=lkc-stg001/topic=*"},

		// --- consumer group scope ---
		{Principal: "User:sa-anlyt01", RoleName: "DeveloperRead",
			CRNPattern: prodAnalytics + "/kafka=lkc-prd002/group=analytics-*"},
		{Principal: "User:sa-orders1", RoleName: "DeveloperRead",
			CRNPattern: prodCore + "/kafka=lkc-prd001/group=orders-service"},
		{Principal: "User:pool-flink1", RoleName: "DeveloperRead",
			CRNPattern: prodAnalytics + "/kafka=lkc-prd002/group=flink-*"},

		// --- schema registry subjects ---
		{Principal: "User:sa-orders1", RoleName: "DeveloperWrite",
			CRNPattern: prod + "/schema-registry=lsrc-prd001/subject=orders-*"},
		{Principal: "User:pool-anlys1", RoleName: "DeveloperRead",
			CRNPattern: prod + "/schema-registry=lsrc-prd001/subject=*"},
		{Principal: "User:u-grace01", RoleName: "DataSteward", CRNPattern: prod},

		// --- transactional id ---
		{Principal: "User:sa-paymnt1", RoleName: "DeveloperWrite",
			CRNPattern: prodCore + "/kafka=lkc-prd001/transactional-id=payments-tx-*"},
	}

	for i := range rbs {
		rbs[i].ID = fmt.Sprintf("rb-mock%03d", i+1)
	}
	return rbs
}
