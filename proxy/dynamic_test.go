package proxy

import (
	"go.uber.org/zap"
	"os"
	"testing"

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
