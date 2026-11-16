package main

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestSecondSignalKillsThenExits(t *testing.T) {
	sigs := make(chan os.Signal, 2)
	shutdown := make(chan struct{}, 1)
	kill := make(chan struct{}, 1)
	exitCode := make(chan int, 1)

	go handleSignals(zap.NewNop(), sigs,
		func() { shutdown <- struct{}{} },
		func() { kill <- struct{}{} },
		func(code int) { exitCode <- code },
		50*time.Millisecond)

	sigs <- syscall.SIGTERM
	select {
	case <-shutdown:
	case <-time.After(time.Second):
		t.Fatal("first signal did not start a graceful shutdown")
	}
	select {
	case <-kill:
		t.Fatal("first signal must not kill")
	case <-exitCode:
		t.Fatal("first signal must not exit")
	case <-time.After(100 * time.Millisecond):
	}

	sigs <- syscall.SIGTERM
	select {
	case <-kill:
	case <-time.After(time.Second):
		t.Fatal("second signal did not kill")
	}
	select {
	case code := <-exitCode:
		assert.Equal(t, 1, code)
	case <-time.After(time.Second):
		t.Fatal("process did not exit after the kill grace period")
	}
	close(sigs)
}
