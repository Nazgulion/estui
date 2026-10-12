package elastic

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"estui/internal/config"
	"estui/internal/logging"
)

func TestMigrationVersionCompatibility(t *testing.T) {
	for _, tt := range []struct {
		source, target string
		allowed        bool
	}{
		{"7.17.16", "7.17.16", true},
		{"7.14.0", "7.17.16", true},
		{"7.17.16", "7.14.0", true},
		{"8.0.0", "8.19.7", true},
		{"8.19.7", "8.17.0", true},
		{"9.0.0", "9.1.2", true},
		{"9.1.2", "9.0.0", true},
		{"7.17.16", "8.17.0", false},
		{"8.17.0", "7.17.16", false},
		{"8.17.0", "9.0.0", false},
		{"9.0.0", "8.17.0", false},
		{"7.17.16", "9.0.0", false},
		{"9.0.0", "7.17.16", false},
		{"7.13.4", "7.13.4", false},
		{"6.8.0", "6.8.0", false},
		{"10.0.0", "10.0.0", false},
		{"8.", "8.17.0", false},
		{"8.17.0", "", false},
		{"9.0.0-SNAPSHOT", "9.0.0-SNAPSHOT", false},
		{"8.not.a.version", "8.17.0", false},
	} {
		t.Run(tt.source+"/"+tt.target, func(t *testing.T) {
			err := ValidateMigrationVersions(tt.source, tt.target)
			if (err == nil) != tt.allowed {
				t.Fatalf("allowed=%t, error=%v", tt.allowed, err)
			}
		})
	}
}

func TestClientVersionHeaders(t *testing.T) {
	for _, version := range []string{"7.17.16", "8.17.0", "9.0.0"} {
		for _, env := range []string{"false", "true"} {
			t.Run(version+"/env="+env, func(t *testing.T) {
				t.Setenv("ELASTIC_CLIENT_APIVERSIONING", env)
				calls := 0
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					want := "application/json"
					if calls > 1 && !strings.HasPrefix(version, "7.") {
						want = "application/vnd.elasticsearch+json;compatible-with=8"
					}
					if r.Header.Get("Accept") != want {
						t.Errorf("%s: Accept=%q, want %q", r.URL, r.Header.Get("Accept"), want)
					}
					if r.ContentLength > 0 && r.Header.Get("Content-Type") != want {
						t.Errorf("%s: Content-Type=%q, want %q", r.URL, r.Header.Get("Content-Type"), want)
					}
					if r.Header.Get("Authorization") != "APIKey test-key" {
						t.Error("lost credentials")
					}
					w.Header().Set("X-Elastic-Product", "Elasticsearch")
					w.Header().Set("Content-Type", "application/json")
					if r.URL.Path == "/" {
						fmt.Fprintf(w, `{"version":{"number":%q}}`, version)
					} else {
						io.WriteString(w, `{}`)
					}
				}))
				defer backend.Close()
				log := logging.New(filepath.Join(t.TempDir(), "test.log"), logging.ERROR)
				defer log.Close()
				client, err := NewClient(config.ESConnection{Hosts: []string{backend.URL}, APIKey: "test-key"}, log)
				if err != nil {
					t.Fatal(err)
				}
				// Exercise both generated SDK requests and our raw safety API path.
				res, err := client.ES().Bulk(strings.NewReader("{}\n{}\n"))
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
				if err := client.jsonRequest(context.Background(), "POST", "/copy/_clone/final", map[string]any{"settings": map[string]any{}}, nil); err != nil {
					t.Fatal(err)
				}
				if calls != 3 {
					t.Fatalf("unexpected request count: %d", calls)
				}
			})
		}
	}
}
