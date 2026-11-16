package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-getter"
	"go.uber.org/zap"
)

// getterMutex serializes go-getter usage: Client.Get configures the package-level default getters
// (getter.Getters) in place, so concurrent Gets from several Dynamic instances are a data race.
var getterMutex sync.Mutex

type Dynamic struct {
	config    *DynamicClusters
	mutex     sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

type DynamicClusters struct {
	Clusters map[string]DynamicCluster
}

type DynamicCluster struct {
	DisableWrites bool
	RedirectTo    string
}

func NewDynamic(url string, log *zap.Logger) (*Dynamic, error) {
	d := &Dynamic{stop: make(chan struct{}), done: make(chan struct{})}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	if err := d.start(url, log); err != nil {
		return nil, err
	}
	return d, nil
}

// Close stops the poller and waits for it to exit. It is safe to call more than once.
func (d *Dynamic) Close() {
	d.closeOnce.Do(func() {
		d.cancel() // abort any in-flight fetch so Close cannot hang on a slow source
		close(d.stop)
		<-d.done
	})
}

func (d *Dynamic) ForAddress(address string) DynamicCluster {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.config != nil && d.config.Clusters != nil {
		if v, ok := d.config.Clusters[address]; ok {
			return v
		}
	}

	return DynamicCluster{}
}

func (d *Dynamic) start(url string, log *zap.Logger) error {
	if url == "" {
		close(d.done)
		return nil
	}

	// poll at least once in the main thread
	if err := d.poll(url, log); err != nil {
		close(d.done)
		return err
	}

	go func() {
		defer close(d.done)
		ticker := time.NewTicker(10 * time.Second) // TODO make configurable
		defer ticker.Stop()
		for {
			select {
			case <-d.stop:
				return
			case <-ticker.C:
				if err := d.poll(url, log); err != nil {
					log.Error("Error polling dynamic URL", zap.String("url", url), zap.Error(err))
				}
			}
		}
	}()

	return nil
}

func (d *Dynamic) poll(url string, log *zap.Logger) error {
	f, err := os.CreateTemp("", "*.json")
	if err != nil {
		return err
	}
	err = f.Close() // Close now, reopen after writing
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close() // need another Close because reopened below
		_ = os.Remove(f.Name())
	}()

	// Copy the input URI to the temporary file before reading. go-getter supports reading from a variety
	// of input URIs, see https://github.com/hashicorp/go-getter.
	pwd, _ := os.Getwd()
	client := &getter.Client{
		Ctx:  d.ctx,
		Src:  url,
		Dst:  f.Name(),
		Pwd:  pwd,
		Mode: getter.ClientModeFile,
	}
	getterMutex.Lock()
	err = client.Get()
	getterMutex.Unlock()
	if err != nil {
		return err
	}

	f, err = os.Open(f.Name())
	if err != nil {
		return err
	}

	body, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	config := DynamicClusters{}
	if err = json.Unmarshal(body, &config); err != nil {
		return err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	newConfig, _ := json.Marshal(config)
	if d.config == nil {
		log.Info("Loaded dynamic config", zap.String("config", string(newConfig)))
	} else {
		oldConfig, _ := json.Marshal(*d.config)
		if !bytes.Equal(oldConfig, newConfig) {
			log.Info("Updated dynamic config", zap.String("config", string(newConfig)), zap.String("old_config", string(oldConfig)))
		}
	}

	d.config = &config
	return nil
}
