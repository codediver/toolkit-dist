// Command ccloud-rbac fetches identity providers, identity pools, role
// bindings and related resources from the Confluent Cloud APIs and exports
// them into a single interactive HTML report showing who has read / write /
// admin access to which resources.
//
// Usage:
//
//	ccloud-rbac --mock -o report.html               # use built-in sample data
//	ccloud-rbac -o report.html                      # real API; reads
//	    CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET or flags
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mk/ccloud-rbac/internal/ccloud"
	"github.com/mk/ccloud-rbac/internal/mock"
	"github.com/mk/ccloud-rbac/internal/model"
	"github.com/mk/ccloud-rbac/internal/report"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		useMock   = flag.Bool("mock", false, "use built-in mock data instead of the real API (no credentials needed)")
		apiKey    = flag.String("api-key", os.Getenv("CONFLUENT_CLOUD_API_KEY"), "Confluent Cloud API key (env CONFLUENT_CLOUD_API_KEY)")
		apiSecret = flag.String("api-secret", os.Getenv("CONFLUENT_CLOUD_API_SECRET"), "Confluent Cloud API secret (env CONFLUENT_CLOUD_API_SECRET)")
		baseURL   = flag.String("base-url", ccloud.DefaultBaseURL, "Confluent Cloud API base URL")
		outPath   = flag.String("o", "ccloud-rbac-report.html", "output HTML file")
		jsonPath  = flag.String("json", "", "optionally also write the raw API snapshot as JSON to this file")
		timeout   = flag.Duration("timeout", 5*time.Minute, "overall timeout for fetching all resources")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var client *ccloud.Client
	if *useMock {
		srv := mock.NewServer()
		defer srv.Close()
		client = ccloud.NewClient(srv.URL, "mock-key", "mock-secret")
		fmt.Fprintln(os.Stderr, "using built-in mock data (Acme Corp sample organization)")
	} else {
		if *apiKey == "" || *apiSecret == "" {
			return fmt.Errorf("no credentials: pass --api-key/--api-secret or set CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET (or use --mock)")
		}
		client = ccloud.NewClient(*baseURL, *apiKey, *apiSecret)
	}

	fmt.Fprintln(os.Stderr, "fetching organization, environments, clusters, principals, identity providers, pools and role bindings...")
	snap, err := ccloud.FetchAll(ctx, client)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "fetched: %d role bindings, %d users, %d service accounts, %d providers, %d pools, %d environments, %d clusters\n",
		len(snap.RoleBindings), len(snap.Users), len(snap.ServiceAccounts),
		len(snap.Providers), len(snap.Pools), len(snap.Environments), len(snap.Clusters))

	if *jsonPath != "" {
		if err := writeSnapshotJSON(*jsonPath, snap); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "wrote snapshot:", *jsonPath)
	}

	rep := model.Build(snap, time.Now().UTC().Format(time.RFC3339))
	html, err := report.Render(rep)
	if err != nil {
		return err
	}
	if err := os.WriteFile(*outPath, html, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", *outPath, err)
	}
	fmt.Fprintln(os.Stderr, "wrote report:", *outPath)
	return nil
}
