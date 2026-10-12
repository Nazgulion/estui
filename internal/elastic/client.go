package elastic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"estui/internal/config"
	"estui/internal/logging"
)

type Client struct {
	es  *elasticsearch.Client
	cfg config.ESConnection
}

// Use native JSON on 7.x and the v8 REST contract on 8.x/9.x. Selecting at
// the wire boundary also overrides ELASTIC_CLIENT_APIVERSIONING, which would
// otherwise make the v8 SDK send unsupported compatible-with=8 headers to 7.x.
// apiVersion is set during connection setup, before this client is published.
type versionTransport struct {
	base       http.RoundTripper
	apiVersion int
}

func (t *versionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	mediaType := "application/json"
	if t.apiVersion == 8 {
		mediaType = "application/vnd.elasticsearch+json;compatible-with=8"
	}
	req.Header.Set("Accept", mediaType)
	if req.Body != nil && req.Body != http.NoBody {
		req.Header.Set("Content-Type", mediaType)
	} else {
		req.Header.Del("Content-Type")
	}
	return t.base.RoundTrip(req)
}

func NewClient(conn config.ESConnection, log *logging.Logger) (*Client, error) {
	if len(conn.Hosts) != 1 {
		return nil, fmt.Errorf("exactly one cluster endpoint is required; use a load balancer for multiple nodes")
	}

	endpoint, err := url.Parse(conn.Hosts[0])
	if err != nil || endpoint.Hostname() == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("endpoint must be an HTTP(S) URL without embedded credentials, query, or fragment")
	}
	if conn.Insecure {
		return nil, fmt.Errorf("insecure_skip_verify is not permitted; configure a trusted CA instead")
	}
	esCfg := elasticsearch.Config{
		Addresses:    conn.Hosts,
		DisableRetry: true,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		},
	}

	if conn.Username != "" {
		esCfg.Username = conn.Username
		esCfg.Password = conn.Password
	}
	if conn.APIKey != "" {
		esCfg.APIKey = conn.APIKey
	}

	if conn.CACert != "" {
		caCert, err := os.ReadFile(conn.CACert)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("CA certificate contains no valid certificates")
		}

		esCfg.Transport = &http.Transport{
			TLSClientConfig:       &tls.Config{RootCAs: caCertPool, MinVersion: tls.VersionTLS12},
			ResponseHeaderTimeout: 30 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
			DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		}
	}

	log.Debug("elastic", "creating client for %s on %v", conn.Name, conn.Hosts)
	transport := &versionTransport{base: esCfg.Transport}
	esCfg.Transport = transport

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("create ES client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := es.Info(es.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("connect to ES: %w", err)
	}
	defer info.Body.Close()
	if info.IsError() {
		return nil, fmt.Errorf("connect to ES: HTTP %d", info.StatusCode)
	}
	var identity struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := json.NewDecoder(info.Body).Decode(&identity); err != nil {
		return nil, fmt.Errorf("read Elasticsearch version: %w", err)
	}
	major, _, err := parseReleaseVersion(identity.Version.Number)
	if err != nil {
		return nil, err
	}
	if major == 8 || major == 9 {
		transport.apiVersion = 8
	}

	log.Info("elastic", "connected to %s at %v", conn.Name, conn.Hosts)

	return &Client{es: es, cfg: conn}, nil
}

func (c *Client) ES() *elasticsearch.Client {
	return c.es
}

func (c *Client) Name() string {
	return c.cfg.Name
}
