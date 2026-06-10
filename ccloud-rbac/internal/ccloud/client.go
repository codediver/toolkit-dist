package ccloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// DefaultBaseURL is the public Confluent Cloud API endpoint.
const DefaultBaseURL = "https://api.confluent.cloud"

// pageSize is the maximum page size accepted by the list endpoints.
const pageSize = 100

// Client is a read-only Confluent Cloud API client authenticated with a
// Cloud API key (HTTP Basic auth).
type Client struct {
	BaseURL   string
	APIKey    string
	APISecret string
	HTTP      *http.Client
}

// NewClient returns a client for the given base URL and Cloud API key.
func NewClient(baseURL, apiKey, apiSecret string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		BaseURL:   baseURL,
		APIKey:    apiKey,
		APISecret: apiSecret,
		HTTP:      &http.Client{Timeout: 30 * time.Second},
	}
}

// apiError reports a non-2xx response.
type apiError struct {
	Status int
	URL    string
	Body   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("confluent cloud api: %s returned %d: %s", e.URL, e.Status, e.Body)
}

// IsNotAllowed reports whether err is an API error with a 4xx status that
// indicates the query (not the credentials) was rejected, e.g. an
// unsupported crn_pattern wildcard.
func IsNotAllowed(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Status >= 400 && ae.Status < 500
}

// getJSON performs one GET request and decodes the JSON response into out.
func (c *Client) getJSON(ctx context.Context, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.APIKey, c.APISecret)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	const maxBody = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return fmt.Errorf("GET %s: read body: %w", rawURL, err)
	}
	if len(body) == maxBody {
		return fmt.Errorf("GET %s: response exceeds %d byte limit", rawURL, maxBody)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &apiError{Status: resp.StatusCode, URL: rawURL, Body: truncate(string(body), 300)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("GET %s: decode: %w", rawURL, err)
	}
	return nil
}

// listAll fetches every page of a list endpoint, following the
// metadata.next links until exhausted.
func listAll[T any](ctx context.Context, c *Client, path string, query url.Values) ([]T, error) {
	// Copy the query so the caller's map is not mutated.
	q := url.Values{}
	for k, vs := range query {
		q[k] = append([]string(nil), vs...)
	}
	q.Set("page_size", fmt.Sprint(pageSize))
	next := c.BaseURL + path + "?" + q.Encode()

	var all []T
	for next != "" {
		var page listEnvelope[T]
		if err := c.getJSON(ctx, next, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Data...)
		next = page.Metadata.Next
		if next != "" && !isSameHost(next, c.BaseURL) {
			// The API returns absolute next-links; if they point at the
			// public host while we target a mock server, rewrite the host.
			next = rewriteHost(next, c.BaseURL)
		}
	}
	return all, nil
}

func isSameHost(rawURL, base string) bool {
	u, err1 := url.Parse(rawURL)
	b, err2 := url.Parse(base)
	return err1 == nil && err2 == nil && u.Host == b.Host
}

func rewriteHost(rawURL, base string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	b, err := url.Parse(base)
	if err != nil {
		return rawURL
	}
	u.Scheme = b.Scheme
	u.Host = b.Host
	return u.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
