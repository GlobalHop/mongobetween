package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/config"
)

func main() {
	c := config.ParseFlags()
	run(c)
}

func run(config *config.Config) {
	proxies, err := config.Proxies(config.Logger())
	log := config.Logger()

	if err != nil {
		log.Fatal("Startup error", zap.Error(err))
	}

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		config.CloseUpstreams()
	}()

	for _, p := range proxies {
		p := p
		wg.Add(1)
		go func() {
			err := p.Run()
			if err != nil {
				log.Error("Error", zap.Error(err))
			}
			wg.Done()
		}()
	}

	shutdown := func() {
		for _, p := range proxies {
			go p.ShutdownWithTimeout(config.ShutdownTimeout())
		}
	}
	kill := func() {
		for _, p := range proxies {
			p.Kill()
		}
	}
	shutdownOnSignal(log, shutdown, kill)

	log.Info("Running")
}

func shutdownOnSignal(log *zap.Logger, shutdownFunc func(), killFunc func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go handleSignals(log, c, shutdownFunc, killFunc, os.Exit, killGracePeriod)
}

// killGracePeriod is how long the process may take to finish after a second signal force closed everything.
const killGracePeriod = 2 * time.Second

// handleSignals starts a graceful shutdown on the first signal. A second signal force closes client connections and
// aborts in-flight upstream requests (killFunc), then exits with status 1 after grace if the process is still alive.
func handleSignals(log *zap.Logger, c <-chan os.Signal, shutdownFunc func(), killFunc func(), exit func(int), grace time.Duration) {
	shutdownAttempted := false
	killed := false
	for sig := range c {
		log.Info("Signal", zap.String("signal", sig.String()))

		if !shutdownAttempted {
			log.Info("Shutting down")
			go shutdownFunc()
			shutdownAttempted = true

			if sig == os.Interrupt {
				time.AfterFunc(1*time.Second, func() {
					fmt.Println("Ctrl-C again to kill incoming connections")
				})
			}
		} else if !killed {
			killed = true
			log.Warn("Terminating", zap.Duration("exit_after", grace))
			_ = log.Sync() // #nosec
			killFunc()
			time.AfterFunc(grace, func() {
				log.Warn("Exiting")
				_ = log.Sync() // #nosec
				exit(1)
			})
		}
	}
}
