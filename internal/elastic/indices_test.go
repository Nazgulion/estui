package elastic

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestListIndicesSortingMetadata(t *testing.T) {
	client := mockClient(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Query().Get("bytes") != "b" || !strings.Contains(r.URL.Query().Get("h"), "creation.date") {
			t.Fatalf("missing exact size or creation date query: %s", r.URL)
		}
		return response(200, `[
			{"index":"large","store.size":"1099511627777","creation.date":"1700000000123"},
			{"index":"empty","store.size":"0","creation.date":"1700000000456"},
			{"index":"closed","status":"close","store.size":null,"creation.date":"1700000000000"},
			{"index":"missing"},
			{"index":"unknown","store.size":"-","creation.date":"-"},
			{"index":".hidden","store.size":"12","creation.date":"1700000000000"}
		]`), nil
	})
	indices, err := client.ListIndices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(indices) != 5 {
		t.Fatalf("unexpected visible indices: %+v", indices)
	}
	if indices[0].SizeBytes == nil || *indices[0].SizeBytes != 1099511627777 || indices[0].CreatedAt == nil || *indices[0].CreatedAt != 1700000000123 {
		t.Fatalf("lost numeric metadata: %+v", indices[0])
	}
	if indices[1].SizeBytes == nil || *indices[1].SizeBytes != 0 || indices[1].SizeHuman != "0 B" {
		t.Fatalf("zero bytes treated as unknown: %+v", indices[1])
	}
	for _, index := range indices[2:] {
		if index.SizeBytes != nil || index.SizeHuman != "" {
			t.Fatalf("unknown size became zero: %+v", index)
		}
	}
	if indices[2].CreatedAt == nil || indices[3].CreatedAt != nil || indices[4].CreatedAt != nil {
		t.Fatal("missing creation dates not preserved")
	}
}
