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

// updateCursorPins records or evicts cursor pins after a reply: a non-zero response cursor pins to server under the
// response namespace (falling back to the request's), a zero cursor on a getMore evicts it, and killCursors evicts
// every ID it names.
func updateCursorPins(cursors *cursorCache, req, res Operation, server driver.Server) {
	requestCursorID, _ := req.CursorID()
	requestNS := req.CursorNamespace()
	if responseCursorID, ok := res.CursorID(); ok {
		if responseCursorID != 0 {
			responseNS := res.CursorNamespace()
			if responseNS == "" {
				responseNS = requestNS
			}
			cursors.add(responseCursorID, responseNS, server)
		} else if requestCursorID != 0 {
			cursors.remove(requestCursorID, requestNS)
		}
	}
	for _, id := range req.KillCursorIDs() {
		cursors.remove(id, requestNS)
	}
}
