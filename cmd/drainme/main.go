// Command drainme is the whole service in one binary (D68). `serve` runs it; `egress` is the allow-listing
// proxy that is serve's only way out (D93); `healthcheck [host:port]` is the container probe (D52); `init-vault`
// opens the funded vault once (D69); status, backup, recover and reconcile are the operator's tools (D101); `work`
// is the local CPU proof-of-work fallback (D104).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/api"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/config"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/egress"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ingest"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/ledger"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanoclient"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/nanocrypto"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/payout"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/pow"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/site"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/stats"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/internal/sysinfo"
	"github.com/Tristan-Phillips/drainme_madebytrap_lol-carbon/web"
)

const (
	// inside the container only; cloudflared reaches it over the private bridge (D56)
	listenAddr = ":8080"
	// the egress proxy, reached by serve as HTTPS_PROXY=http://drainme-egress:3128 (D93)
	egressAddr = ":3128"
	// the local work server, reached by serve as http://drainme-work:7076 on the internal network (D104)
	workAddr = ":7076"
	// D16
	flushEvery = 5 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)) // D63: structured, to stdout
	usage := func() {
		fmt.Fprintln(os.Stderr, "usage: drainme serve | egress | work | healthcheck [host:port] | init-vault [<raw> [--resume <hash>]]\n"+
			"               status | backup [<dir>] [--keep <n>] | recover [--recredit] | reconcile [--adopt <hash>]")
		os.Exit(2)
	}
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; {
	case cmd == "serve" && len(args) == 0:
		err = runServe(log)
	case cmd == "egress" && len(args) == 0:
		err = runEgress(log)
	case cmd == "work" && len(args) == 0:
		err = runWork(log)
	case cmd == "init-vault" && len(args) <= 3:
		err = runInitVault(args)
	case cmd == "status" && len(args) == 0,
		cmd == "backup" && len(args) <= 3,
		cmd == "recover" && (len(args) == 0 || len(args) == 1 && args[0] == "--recredit"),
		cmd == "reconcile" && (len(args) == 0 || len(args) == 2 && args[0] == "--adopt"):
		err = runOperator(cmd, args, log)
	case cmd == "healthcheck" && len(args) <= 1:
		target := "127.0.0.1" + listenAddr
		if len(args) == 1 {
			target = args[0]
		}
		err = healthcheck("http://" + target + "/healthz")
	default:
		usage()
	}
	if err != nil {
		log.Error("drainme: exiting", "err", err)
		os.Exit(1)
	}
}

func runServe(log *slog.Logger) error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, cfg, ln, log)
}

func runEgress(log *slog.Logger) error {
	targets, err := config.EgressTargets(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: egressAddr, Handler: egress.Handler(targets, log), ReadHeaderTimeout: 5 * time.Second, MaxHeaderBytes: 8 << 10,
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	go func() {
		<-ctx.Done()
		srv.Close() // tunnels are hijacked connections, which Shutdown would not wait for anyway
	}()
	log.Info("drainme egress: allowing", "targets", targets)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func runWork(log *slog.Logger) error {
	threads, err := config.WorkThreads(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// no write timeout: a search runs until it finds work or its caller gives up, which ends the request
	srv := &http.Server{Addr: workAddr, Handler: pow.Handler(threads, log), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 8 << 10, ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Info("drainme work: serving", "threads", threads)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// serve runs until ctx ends, then shuts down in the order that loses nothing: stop taking requests, flush the
// clicks already accepted, stop the payout worker between sends, close the ledger.
func serve(ctx context.Context, cfg *config.Config, ln net.Listener, log *slog.Logger) error {
	defer ln.Close()
	rep, err := nanocrypto.ParseAddress(cfg.Representative)
	if err != nil {
		return fmt.Errorf("serve: DRAINME_REPRESENTATIVE must be a nano_ address (D8)")
	}
	seed, err := config.ReadSeed(cfg.SeedFile)
	if err != nil {
		return err
	}
	key := nanocrypto.DeriveKey(seed, 0) // D85
	clear(seed[:])                       // only the derived key is needed from here on

	release, err := lockData(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	defer release()
	l, err := ledger.Open(filepath.Join(cfg.DataDir, "ledger.db"), cfg.QueueMax)
	if err != nil {
		return err
	}
	defer l.Close()
	if err := l.Audit(ctx); err != nil {
		return fmt.Errorf("serve: the books do not balance, so nothing starts (D19): %w", err)
	}
	counter, err := ingest.New(ctx, l, cfg.AddressMax, log)
	if err != nil {
		return err
	}
	st := stats.New(l, counter, cfg.ViewerMax, log)
	node := nanoclient.New(cfg.RPCURLs, cfg.WorkURLs, cfg.RPCTimeout)
	node.SetWorkTimeout(cfg.WorkTimeout)
	worker := payout.New(l, node, key, rep, cfg.SendThreshold, log)
	worker.SetSendCeiling(cfg.SendCeiling)
	worker.OnConfirmed(st.Paid)
	worker.SweepIdleAfter(cfg.IdleAfter)
	st.Budget(sysinfo.New("/sys/fs/cgroup"))
	st.SetVault(key.Public().Address())
	// touch data/drainme/under-attack on the host while Cloudflare's Under Attack mode is on (D104)
	st.NoticeFile(filepath.Join(cfg.DataDir, "under-attack"))
	page, err := site.New(web.Files)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Handler: routes(page, api.New(l, counter, st, worker.Wake, worker.ETA, key.Public(), log), st, st.ServeSnapshot),
		// D32: protocol limits only. No handler reads a body; net/http discards at most 256 KB of an unread one
		// before dropping the connection, and ReadTimeout bounds a slow sender. A stream sets its own deadline
		// per write, which overrides WriteTimeout.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       2 * time.Minute, // keep-alive, so a clicking client reuses its connection
		MaxHeaderBytes:    8 << 10,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	srv.RegisterOnShutdown(st.Close)

	var wg sync.WaitGroup
	run := func(fn func(context.Context)) context.CancelFunc {
		c, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go func() { defer wg.Done(); fn(c) }()
		return cancel
	}
	stopCounter := run(func(c context.Context) {
		if err := counter.Run(c, flushEvery); errors.Is(err, ledger.ErrVaultExhausted) {
			st.SetHalted(true)
			log.Error("drainme: HALTED: the vault cannot pay further clicks (D20); clicks are refused", "err", err)
		} else if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("drainme: final click flush failed", "err", err)
		}
	})
	stopWorker := run(func(c context.Context) {
		if err := worker.Run(c); errors.Is(err, payout.ErrHalt) {
			st.SetHalted(true)
			log.Error("drainme: PAYOUTS HALTED, funds intact; clicks still count; run drainme status (README: When payouts stop)", "err", err)
		} else if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("drainme: payout worker stopped", "err", err)
		}
	})
	stopStats := run(func(c context.Context) { st.Run(c) })

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	log.Info("drainme: serving", "addr", ln.Addr().String(), "account", key.Public().Address())

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-served:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil {
		log.Warn("drainme: shutdown cut short", "err", err)
	}
	stopCounter() // after Shutdown: no click can arrive once the last flush has run
	stopWorker()
	stopStats()
	wg.Wait()
	log.Info("drainme: stopped")
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}

// routes is everything the server answers, and nothing else.
func routes(page, money, stream http.Handler, snapshot http.HandlerFunc) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", page)
	mux.Handle("/click/", money)
	mux.Handle("/withdraw/", money)
	mux.Handle("GET /events", stream)
	mux.HandleFunc("GET /snapshot", snapshot) // for viewers past the stream cap (D104)
	// alive and serving, nothing more: a halted payout worker is not a reason to restart (D64)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func runOperator(cmd string, args []string, log *slog.Logger) error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	path := filepath.Join(cfg.DataDir, "ledger.db")
	if cmd == "status" || cmd == "backup" {
		l, err := ledger.OpenReader(path)
		if err != nil {
			return err
		}
		defer l.Close()
		if cmd == "backup" {
			dir, keep, err := parseBackupArgs(args, filepath.Join(cfg.DataDir, "backups"))
			if err != nil {
				return err
			}
			_, err = backup(ctx, l, dir, keep, time.Now(), os.Stdout)
			return err
		}
		pub, err := account(cfg)
		if err != nil {
			return err
		}
		return status(ctx, l, endpoints(cfg), nanoclient.New(cfg.RPCURLs, nil, cfg.RPCTimeout), pub, cfg.SendThreshold, os.Stdout)
	}

	pub, err := account(cfg)
	if err != nil {
		return err
	}
	release, err := lockData(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("%s: %w", cmd, err)
	}
	defer release()
	l, err := ledger.Open(path, cfg.QueueMax)
	if err != nil {
		return err
	}
	defer l.Close()
	if err := l.Audit(ctx); err != nil {
		return fmt.Errorf("%s: the books do not balance, so nothing is changed: %w", cmd, err)
	}
	if cmd == "recover" {
		return recoverSend(ctx, l, endpoints(cfg), pub, len(args) == 1, os.Stdout, log)
	}
	var adopt *nanocrypto.Hash
	if len(args) == 2 {
		h, err := nanocrypto.ParseHash(args[1])
		if err != nil {
			return errors.New("reconcile: --adopt takes the 64-character frontier hash reconcile printed")
		}
		adopt = &h
	}
	return reconcile(ctx, l, endpoints(cfg), pub, adopt, os.Stdout, log)
}

// parseBackupArgs reads [<dir>] [--keep <n>]
func parseBackupArgs(args []string, dir string) (string, int, error) {
	keep := 0
	if n := len(args); n >= 2 && args[n-2] == "--keep" {
		k, err := strconv.Atoi(args[n-1])
		if err != nil || k < 1 || k > 10_000 {
			return "", 0, errors.New("backup: --keep takes how many copies to keep, from 1 to 10000")
		}
		keep, args = k, args[:n-2]
	}
	switch {
	case len(args) == 1 && !strings.HasPrefix(args[0], "-"):
		dir = args[0]
	case len(args) != 0:
		return "", 0, errors.New("usage: drainme backup [<dir>] [--keep <n>]")
	}
	return dir, keep, nil
}

// healthcheck is the container probe: the scratch image has no curl (D51, D52).
func healthcheck(url string) error {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(url)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("healthcheck: HTTP %d", resp.StatusCode)
	}
	return nil
}
