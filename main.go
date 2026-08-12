package main

import (
	"flag"
	"fmt"
	"os"

	"estui/internal/config"
	"estui/internal/elastic"
	"estui/internal/logging"
	"estui/internal/web"
)

func main() {
	cfgPath := "config.yaml"
	port := 8080

	flag.IntVar(&port, "port", 8080, "web server port")
	flag.Parse()

	if len(flag.Args()) > 0 {
		cfgPath = flag.Args()[0]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := logging.New(cfg.LogFile, logging.ParseLevel(cfg.LogLevel))
	defer log.Close()

	log.Info("app", "starting estui web on port %d (log: %s, level: %s)", port, cfg.LogFile, cfg.LogLevel)

	if len(cfg.Sources) == 0 {
		log.Error("app", "no sources configured")
		fmt.Fprintf(os.Stderr, "No source Elasticsearch configured\n")
		os.Exit(1)
	}
	if len(cfg.Targets) == 0 {
		log.Error("app", "no targets configured")
		fmt.Fprintf(os.Stderr, "No target Elasticsearch configured\n")
		os.Exit(1)
	}

	srcClient, err := elastic.NewClient(cfg.Sources[0], log)
	if err != nil {
		log.Error("app", "source connection failed: %v", err)
		fmt.Fprintf(os.Stderr, "Source connection failed: %v\n", err)
		os.Exit(1)
	}
	log.Info("app", "connected to source: %s", cfg.Sources[0].Name)

	tgtClient, err := elastic.NewClient(cfg.Targets[0], log)
	if err != nil {
		log.Error("app", "target connection failed: %v", err)
		fmt.Fprintf(os.Stderr, "Target connection failed: %v\n", err)
		os.Exit(1)
	}
	log.Info("app", "connected to target: %s", cfg.Targets[0].Name)

	server := web.New(
		srcClient, tgtClient,
		cfg.Sources[0].ReindexURL(), cfg.Targets[0].ReindexURL(),
		cfg.Sources[0], cfg.Targets[0], cfgPath,
		log, port,
	)

	fmt.Printf("estui web server → http://localhost:%d\n", port)
	if err := server.Start(); err != nil {
		log.Error("app", "server error: %v", err)
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
