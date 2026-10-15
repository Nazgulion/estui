package elastic

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

func (c *Client) jsonRequest(ctx context.Context, method, endpoint string, body any, result any) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.es.Perform(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, endpoint, resp.StatusCode, data)
	}
	if result != nil {
		if err := json.Unmarshal(data, result); err != nil {
			return fmt.Errorf("%s: invalid response: %w", endpoint, err)
		}
	}
	return nil
}

// Setting accepts either nested settings or Elasticsearch's flat settings format.
func Setting(detail *IndexDetail, key string) any {
	if v, ok := detail.Settings["index."+key]; ok {
		return v
	}
	current, ok := detail.Settings["index"].(map[string]any)
	if !ok {
		return nil
	}
	parts := strings.Split(key, ".")
	for i, p := range parts {
		if v, ok := current[strings.Join(parts[i:], ".")]; ok {
			return v
		}
		v := current[p]
		if i == len(parts)-1 {
			return v
		}
		current, ok = v.(map[string]any)
		if !ok {
			return nil
		}
	}
	return nil
}
func WriteBlocked(detail *IndexDetail) bool {
	for _, key := range []string{"blocks.write", "blocks.read_only", "blocks.read_only_allow_delete"} {
		if fmt.Sprint(Setting(detail, key)) == "true" {
			return true
		}
	}
	return false
}
func SourceIdentity(detail *IndexDetail) (string, error) {
	uuid, err := ReadableSourceIdentity(detail)
	if err != nil {
		return "", err
	}
	if !WriteBlocked(detail) {
		return "", fmt.Errorf("no enforced source write block found; apply index.blocks.write or explicitly choose live copy (no write block; later writes are not synchronized)")
	}
	return uuid, nil
}

// ReadableSourceIdentity validates a complete readable source without requiring
// or changing a write block. Live copies must never use this as proof of stability.
func ReadableSourceIdentity(detail *IndexDetail) (string, error) {
	uuid, _ := Setting(detail, "uuid").(string)
	if uuid == "" {
		return "", fmt.Errorf("source has no index UUID")
	}
	if source, ok := detail.Mappings["_source"].(map[string]any); ok {
		if fmt.Sprint(source["enabled"]) == "false" || source["includes"] != nil || source["excludes"] != nil {
			return "", fmt.Errorf("filtered or disabled _source cannot guarantee a complete copy")
		}
	}
	return uuid, nil
}

func (c *Client) BlockWrites(ctx context.Context, index string) error {
	var response struct {
		Acknowledged       bool              `json:"acknowledged"`
		ShardsAcknowledged bool              `json:"shards_acknowledged"`
		Indices            []json.RawMessage `json:"indices"`
	}
	if err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index)+"/_block/write", nil, &response); err != nil {
		return err
	}
	if response.Acknowledged {
		if response.ShardsAcknowledged {
			return nil
		}
		// Elasticsearch returns acknowledged=true, shards_acknowledged=false,
		// indices=[] when the write block was already applied. This is a no-op,
		// not a failed shard acknowledgement. Require the exact empty list and
		// re-read the explicit write block; do not accept partial/missing results.
		if response.Indices != nil && len(response.Indices) == 0 {
			detail, err := c.GetIndexDetail(ctx, index)
			if err != nil {
				return err
			}
			if fmt.Sprint(Setting(detail, "blocks.write")) == "true" {
				return nil
			}
		}
	}
	return fmt.Errorf("write block for %s was not fully acknowledged", index)
}

// SetWriteBlock also works on closed indices. Before reading an open index for
// verification, use BlockWrites as well to drain any in-flight writes.
func (c *Client) SetWriteBlock(ctx context.Context, index string, blocked bool) error {
	var response struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := c.jsonRequest(ctx, "PUT", "/"+url.PathEscape(index)+"/_settings", map[string]any{"index.blocks.write": blocked}, &response); err != nil {
		return err
	}
	if !response.Acknowledged {
		return fmt.Errorf("write-block setting for %s was not acknowledged", index)
	}
	return nil
}

func (c *Client) RefreshIndex(ctx context.Context, index string) error {
	var response struct {
		Shards struct {
			Total  int `json:"total"`
			Failed int `json:"failed"`
		} `json:"_shards"`
	}
	if err := c.jsonRequest(ctx, "POST", "/"+url.PathEscape(index)+"/_refresh", nil, &response); err != nil {
		return err
	}
	if response.Shards.Total == 0 || response.Shards.Failed != 0 {
		return fmt.Errorf("refresh for %s did not succeed on all shards", index)
	}
	return nil
}

// Creation must not publish aliases or activate lifecycle actions from templates.
// Check all matching templates, including components, conservatively (even shadowed ones).
func (c *Client) CheckTemplates(ctx context.Context, index string) error {
	var legacy map[string]struct {
		IndexPatterns []string       `json:"index_patterns"`
		Aliases       map[string]any `json:"aliases"`
		Settings      map[string]any `json:"settings"`
	}
	if err := c.jsonRequest(ctx, "GET", "/_template", nil, &legacy); err != nil {
		return err
	}
	unsafe := func(settings, aliases map[string]any) bool {
		if len(aliases) > 0 {
			return true
		}
		raw, _ := json.Marshal(settings)
		return bytes.Contains(raw, []byte("lifecycle"))
	}
	matches := func(patterns []string) bool {
		for _, p := range patterns {
			if ok, _ := path.Match(p, index); ok {
				return true
			}
		}
		return false
	}
	for name, t := range legacy {
		if matches(t.IndexPatterns) && unsafe(t.Settings, t.Aliases) {
			return fmt.Errorf("destination template %s supplies aliases or lifecycle settings", name)
		}
	}
	type template struct {
		Settings map[string]any `json:"settings"`
		Aliases  map[string]any `json:"aliases"`
	}
	var composable struct {
		Templates []struct {
			Name  string `json:"name"`
			Value struct {
				Patterns   []string `json:"index_patterns"`
				Components []string `json:"composed_of"`
				Template   template `json:"template"`
			} `json:"index_template"`
		} `json:"index_templates"`
	}
	if err := c.jsonRequest(ctx, "GET", "/_index_template", nil, &composable); err != nil {
		return err
	}
	if composable.Templates == nil {
		return fmt.Errorf("destination returned no index_templates field")
	}
	for _, t := range composable.Templates {
		if !matches(t.Value.Patterns) {
			continue
		}
		if unsafe(t.Value.Template.Settings, t.Value.Template.Aliases) {
			return fmt.Errorf("destination template %s supplies aliases or lifecycle settings", t.Name)
		}
		for _, name := range t.Value.Components {
			var components struct {
				Values []struct {
					Value struct {
						Template template `json:"template"`
					} `json:"component_template"`
				} `json:"component_templates"`
			}
			if err := c.jsonRequest(ctx, "GET", "/_component_template/"+url.PathEscape(name), nil, &components); err != nil {
				return err
			}
			if len(components.Values) != 1 {
				return fmt.Errorf("cannot verify component template %s", name)
			}
			v := components.Values[0].Value.Template
			if unsafe(v.Settings, v.Aliases) {
				return fmt.Errorf("component template %s supplies aliases or lifecycle settings", name)
			}
		}
	}
	return nil
}

// Fingerprint is a constant-memory multiset hash of canonical JSON documents,
// including ID and routing. Count plus a 256-bit modular sum preserves multiplicity.
// UseNumber avoids rounding large integer values through float64.
type Fingerprint struct {
	Count int64  `json:"count"`
	Hash  string `json:"hash"`
}

// FingerprintAccumulator records the exact documents observed in a completed
// scroll, allowing destination readback without assuming the source is frozen.
type FingerprintAccumulator struct {
	sum   big.Int
	count int64
}

func (a *FingerprintAccumulator) Add(docs []Document) error {
	modulus := new(big.Int).Lsh(big.NewInt(1), 256)
	for _, doc := range docs {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(doc.Source))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		raw, err := json.Marshal([]any{doc.ID, doc.Routing, value})
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		a.sum.Add(&a.sum, new(big.Int).SetBytes(digest[:]))
		a.sum.Mod(&a.sum, modulus)
		a.count++
	}
	return nil
}

func (a *FingerprintAccumulator) Result() Fingerprint {
	return Fingerprint{Count: a.count, Hash: fmt.Sprintf("%064x", &a.sum)}
}

func (c *Client) Fingerprint(ctx context.Context, index, query string) (Fingerprint, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	docs, errs := c.ScrollAll(ctx, index, 1000, query)
	defer func() {
		cancel()
		for range docs {
		}
		for range errs {
		}
	}()
	var accumulator FingerprintAccumulator
	for batch := range docs {
		if err := accumulator.Add(batch); err != nil {
			return Fingerprint{}, err
		}
	}
	for err := range errs {
		if err != nil {
			return Fingerprint{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return Fingerprint{}, err
	}
	return accumulator.Result(), nil
}

func (c *Client) WaitIndexGreen(ctx context.Context, index string) error {
	var response struct {
		Status   string `json:"status"`
		TimedOut bool   `json:"timed_out"`
	}
	if err := c.jsonRequest(ctx, "GET", "/_cluster/health/"+url.PathEscape(index)+"?wait_for_status=green&timeout=30s", nil, &response); err != nil {
		return err
	}
	if response.TimedOut || response.Status != "green" {
		return fmt.Errorf("index %s must have green health before clone/replacement", index)
	}
	return nil
}

func (c *Client) CloneBlocked(ctx context.Context, source, target string) error {
	detail, err := c.GetIndexDetail(ctx, source)
	if err != nil {
		return err
	}
	settings := map[string]any{"index.blocks.write": true, "index.default_pipeline": "_none", "index.final_pipeline": "_none"}
	for _, key := range []string{"number_of_replicas", "auto_expand_replicas"} {
		if value := Setting(detail, key); value != nil {
			settings["index."+key] = value
		}
	}
	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := c.jsonRequest(ctx, "POST", "/"+url.PathEscape(source)+"/_clone/"+url.PathEscape(target), map[string]any{"settings": settings}, &result); err != nil {
		return err
	}
	if !result.Acknowledged {
		return fmt.Errorf("clone outcome is not acknowledged")
	}
	return nil
}
