package proxy

import (
	"go.uber.org/zap"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDynamic(t *testing.T) {
	json := `{
	  "Clusters": {
		":12345": {
		  "DisableWrites": true,
		  "RedirectTo": ""
		},
		"/var/tmp/mongo.sock": {
		  "DisableWrites": false,
		  "RedirectTo": "/var/tmp/another.sock"
		}
	  }
	}`
	f, err := os.CreateTemp("", "*.json")
	assert.Nil(t, err)
	defer func() {
		_ = os.Remove(f.Name())
	}()
	_, err = f.Write([]byte(json))
	assert.Nil(t, err)
	err = f.Close()
	assert.Nil(t, err)

	d, err := NewDynamic(f.Name(), zap.L())
	assert.Nil(t, err)
	t.Cleanup(d.Close)

	dy := d.ForAddress(":12345")
	assert.True(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "")

	dy = d.ForAddress("/var/tmp/mongo.sock")
	assert.False(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "/var/tmp/another.sock")

	dy = d.ForAddress("non-existent")
	assert.False(t, dy.DisableWrites)
	assert.Equal(t, dy.RedirectTo, "")
}

func TestDynamicCloseStopsPollerAndIsIdempotent(t *testing.T) {
	f, err := os.CreateTemp("", "*.json")
	assert.Nil(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.Write([]byte(`{"Clusters":{}}`))
	assert.Nil(t, err)
	assert.Nil(t, f.Close())

	d, err := NewDynamic(f.Name(), zap.L())
	assert.Nil(t, err)
	d.Close()
	d.Close()
	select {
	case <-d.done:
	default:
		t.Fatal("poller still running after Close")
	}
}

func TestDynamicCloseAbortsInFlightFetch(t *testing.T) {
	var block int32
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&block) == 0 {
			_, _ = w.Write([]byte(`{"Clusters":{}}`))
			return
		}
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	d, err := NewDynamic(srv.URL+"/c.json", zap.L())
	assert.Nil(t, err)

	atomic.StoreInt32(&block, 1)
	// Simulate an in-flight poll blocked on the server.
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		_ = d.poll(srv.URL+"/c.json", zap.L())
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch did not start")
	}

	closed := make(chan struct{})
	go func() { d.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on in-flight fetch")
	}
	select {
	case <-pollDone:
	case <-time.After(time.Second):
		t.Fatal("in-flight fetch not aborted")
	}
}
