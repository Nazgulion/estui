package elastic

import (
	"context"
	"net/http"
	"testing"
)

func TestReviewCountRejectsInvalidResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"forbidden", 403, `{"error":"forbidden"}`},
		{"missing", 200, `{}`},
		{"negative", 200, `{"count":-1}`},
		{"partial", 200, `{"count":10,"_shards":{"failed":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := mockClient(t, func(*http.Request) (*http.Response, error) { return response(tc.status, tc.body), nil })
			if n, err := client.Count(context.Background(), "test"); err == nil {
				t.Fatalf("invalid count accepted: count=%d", n)
			}
		})
	}
}

func TestReviewDiskRejectsInvalidResults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"forbidden", 403, `{"error":"forbidden"}`},
		{"missing", 200, `{}`},
		{"partial", 200, `{"_nodes":{"failed":1},"nodes":{"fs":{"total_in_bytes":100,"available_in_bytes":80}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := mockClient(t, func(*http.Request) (*http.Response, error) { return response(tc.status, tc.body), nil })
			if disk, err := client.GetDiskStats(context.Background()); err == nil {
				t.Fatalf("invalid disk stats shown as success: %+v", disk)
			}
		})
	}
}

func TestReviewDiskUsesEffectiveWatermarks(t *testing.T) {
	client := mockClient(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/_cluster/stats" {
			return response(200, `{"nodes":{"fs":{"total_in_bytes":100,"available_in_bytes":80}}}`), nil
		}
		return response(200, `{"defaults":{"cluster":{"routing":{"allocation":{"disk":{"watermark":{"low":"85%","high":"90%","flood_stage":"95%"}}}}}},"persistent":{"cluster.routing.allocation.disk.watermark.high":"80%"},"transient":{"cluster":{"routing":{"allocation":{"disk":{"watermark":{"low":"70%"}}}}}}}`), nil
	})
	disk, err := client.GetDiskStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if disk.WatermarkLow != "70%" || disk.WatermarkHigh != "80%" || disk.WatermarkFlood != "95%" {
		t.Fatalf("ignored configured watermarks: %+v", disk)
	}
}
