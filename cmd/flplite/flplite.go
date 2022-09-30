package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/mariomac/flplite/pkg/operational"
	"github.com/mariomac/flplite/pkg/pipe"
	"github.com/sirupsen/logrus"
)

func main() {
	log := logrus.WithField("component", "main()")
	config := flag.String("config", "", "configuration file (in json)")
	flag.Parse()

	if config == nil || *config == "" {
		fmt.Fprintln(os.Stderr, "ERROR: missing --config argument")
		flag.Usage()
		os.Exit(-1)
	}

	cfgFile, err := os.ReadFile(*config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR opening %q: %s\n", *config, err)
		os.Exit(-1)
	}

	cfg := pipe.ConfigFileStruct{}
	if err := json.Unmarshal(cfgFile, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR parsing %q: %s\n", *config, err)
		os.Exit(-1)
	}

	lvl, err := logrus.ParseLevel(cfg.LogLevel)
	if err == nil {
		logrus.SetLevel(lvl)
		logrus.Debug("setting log level", lvl.String())
	}

	pipeNode, err := pipe.Build(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR starting pipeline: %s\n", err)
		os.Exit(-1)
	}

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()

	// some operational stuff here
	if cfg.Profile != nil && cfg.Profile.Port != 0 {
		go func() {
			log.WithField("port", cfg.Profile.Port).Info("starting PProf HTTP listener")
			log.WithError(http.ListenAndServe(fmt.Sprintf(":%d", cfg.Profile.Port), nil)).
				Error("PProf HTTP listener stopped working")
		}()
	}
	hp := 8080
	if cfg.Health != nil && cfg.Health.Port > 0 {
		hp = cfg.Health.Port
	}
	// TODO: proper health check
	operational.NewHealthServer(hp, func() error { return nil }, func() error { return nil })

	pipeNode.StartCtx(ctx)

	select {
	case <-c:
		cancel()
	case <-ctx.Done():
	}

}
