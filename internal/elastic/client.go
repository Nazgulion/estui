package elastic

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
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

func NewClient(conn config.ESConnection, log *logging.Logger) (*Client, error) {
	esCfg := elasticsearch.Config{
		Addresses: conn.Hosts,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
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
		caCertPool.AppendCertsFromPEM(caCert)

		esCfg.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: caCertPool, InsecureSkipVerify: conn.Insecure},
			DialContext:     (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		}
	}

	log.Debug("elastic", "creating client for %s on %v", conn.Name, conn.Hosts)

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("create ES client: %w", err)
	}

	info, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("connect to ES: %w", err)
	}
	defer info.Body.Close()

	log.Info("elastic", "connected to %s at %v", conn.Name, conn.Hosts)

	return &Client{es: es, cfg: conn}, nil
}

func (c *Client) ES() *elasticsearch.Client {
	return c.es
}

func (c *Client) Name() string {
	return c.cfg.Name
}
