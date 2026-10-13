// Command obsd runs the observability backend server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/robertramos/developer-observability-stack/internal/app"
	"github.com/robertramos/developer-observability-stack/internal/auth"
	"github.com/robertramos/developer-observability-stack/internal/config"
)

var version = "0.1.0-dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "obsd:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("obsd", flag.ContinueOnError)
	cfgPath := fs.String("config", os.Getenv("OBS_CONFIG"), "path to a JSON config file (optional)")
	showVersion := fs.Bool("version", false, "print the version and exit")
	hashKey := fs.String("hash-key", "", "print the SHA-256 digest of an API key for use as key_sha256 and exit")
	checkConfig := fs.Bool("check-config", false, "validate the configuration and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case *showVersion:
		fmt.Println("obsd", version)
		return nil
	case *hashKey != "":
		fmt.Println(auth.HashKey(*hashKey))
		return nil
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *checkConfig {
		fmt.Println("configuration OK")
		return nil
	}
	log := newLogger(cfg)
	a, err := app.New(cfg, log)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		a.Shutdown(context.Background())
		return err
	}
	log.Info("obsd listening", "addr", ln.Addr().String(), "version", version, "data_dir", cfg.DataDir, "auth", cfg.Auth.Enabled)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return a.Serve(ctx, ln)
}

func newLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	level.UnmarshalText([]byte(strings.ToLower(cfg.LogLevel)))
	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
