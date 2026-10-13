// Command loadgen sends synthetic logs, metrics and OTLP traces to obsd
// and can serve a Prometheus exposition endpoint for scrape testing.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/loadgen"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	cfg := loadgen.Config{}
	fs.StringVar(&cfg.URL, "url", "http://127.0.0.1:8428", "obsd base URL")
	fs.StringVar(&cfg.APIKey, "api-key", os.Getenv("OBS_API_KEY"), "API key with write scope (or OBS_API_KEY)")
	fs.DurationVar(&cfg.Duration, "duration", 10*time.Second, "how long to generate load")
	fs.Float64Var(&cfg.Rate, "rate", 5, "requests per second per signal")
	fs.IntVar(&cfg.BatchSize, "batch", 100, "log entries per request (traces use batch/20)")
	fs.IntVar(&cfg.Concurrency, "concurrency", 4, "concurrent senders")
	fs.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "random seed")
	fs.BoolVar(&cfg.Sync, "sync", false, "wait for persistence on every request (?sync=true)")
	signals := fs.String("signals", "logs,metrics,traces", "comma-separated signals to send")
	serve := fs.String("serve-metrics", "", "serve a synthetic Prometheus /metrics endpoint on this address (e.g. 127.0.0.1:9101) instead of sending load")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *serve != "" {
		return serveExposition(ctx, *serve, cfg.Seed)
	}
	for _, s := range strings.Split(*signals, ",") {
		switch s = strings.TrimSpace(s); s {
		case "logs", "metrics", "traces":
			cfg.Signals = append(cfg.Signals, s)
		case "":
		default:
			return fmt.Errorf("unknown signal %q", s)
		}
	}
	st, err := loadgen.Run(ctx, cfg, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(st, "", "  ")
	fmt.Println(string(out))
	if st.Errors > 0 {
		return fmt.Errorf("%d requests failed", st.Errors)
	}
	return nil
}

func serveExposition(ctx context.Context, addr string, seed int64) error {
	b := loadgen.NewBuilder(seed)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write(b.Exposition())
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	fmt.Fprintln(os.Stderr, "serving synthetic metrics on http://"+addr+"/metrics")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
