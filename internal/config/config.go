package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type ESConnection struct {
	Name              string   `yaml:"name"`
	Hosts             []string `yaml:"hosts"`
	RemoteReindexHost string   `yaml:"remote_reindex_host"`
	Username          string   `yaml:"username"`
	Password          string   `yaml:"password"`
	APIKey            string   `yaml:"api_key"`
	CACert            string   `yaml:"ca_cert"`
	Insecure          bool     `yaml:"insecure_skip_verify"`
}

func (c ESConnection) ReindexURL() string {
	if c.RemoteReindexHost != "" {
		return c.RemoteReindexHost
	}
	if len(c.Hosts) > 0 {
		return c.Hosts[0]
	}
	return ""
}

type Config struct {
	LogLevel string         `yaml:"log_level"`
	LogFile  string         `yaml:"log_file"`
	Port     int            `yaml:"port"`
	Sources  []ESConnection `yaml:"sources"`
	Targets  []ESConnection `yaml:"targets"`
}

func DefaultConfig() *Config {
	return &Config{
		LogLevel: "info",
		LogFile:  "estui.log",
		Port:     8080,
	}
}

func Load(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			dir := filepath.Dir(path)
			if dir != "." {
				if mkErr := os.MkdirAll(dir, 0755); mkErr != nil {
					return nil, fmt.Errorf("create config directory: %w", mkErr)
				}
			}
			f, cErr := os.Create(path)
			if cErr != nil {
				return nil, fmt.Errorf("create default config: %w", cErr)
			}
			encoder := yaml.NewEncoder(f)
			encoder.SetIndent(2)
			if encErr := encoder.Encode(cfg); encErr != nil {
				f.Close()
				return nil, fmt.Errorf("write default config: %w", encErr)
			}
			f.Close()
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.LogFile == "" {
		cfg.LogFile = "estui.log"
	}
	if cfg.Port == 0 {
		cfg.Port = 8080
	}

	return cfg, nil
}

func (c *Config) FindSource(name string) (*ESConnection, error) {
	for i := range c.Sources {
		if c.Sources[i].Name == name {
			return &c.Sources[i], nil
		}
	}
	return nil, fmt.Errorf("source %q not found", name)
}

func (c *Config) FindTarget(name string) (*ESConnection, error) {
	for i := range c.Targets {
		if c.Targets[i].Name == name {
			return &c.Targets[i], nil
		}
	}
	return nil, fmt.Errorf("target %q not found", name)
}
