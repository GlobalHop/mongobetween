package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/x/mongo/driver"

	"github.com/coinbase/mongobetween/lruttl"
)

// on a 64-bit machine, 1 million cursors uses around 480mb of memory
const maxCursors = 1024 * 1024

// one day expiry
const cursorExpiry = 24 * time.Hour

type cursorCache struct {
	c *lruttl.Cache
}

func newCursorCache() *cursorCache {
	return &cursorCache{
		c: lruttl.New(maxCursors, cursorExpiry),
	}
}

func (c *cursorCache) count() int {
	return c.c.Len()
}

func (c *cursorCache) peek(cursorID int64, ns string) (server driver.Server, ok bool) {

	v, ok := c.c.Peek(buildKey(cursorID, ns))
	if !ok {
		return
	}
	return v.(driver.Server), true
}

func (c *cursorCache) add(cursorID int64, ns string, server driver.Server) {
	c.c.Add(buildKey(cursorID, ns), server)
}

func (c *cursorCache) remove(cursorID int64, ns string) {
	c.c.Remove(buildKey(cursorID, ns))
}

func buildKey(cursorID int64, ns string) string {
	return fmt.Sprintf("%d-%s", cursorID, ns)
}
