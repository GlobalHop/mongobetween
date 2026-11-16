package mongo

import "go.mongodb.org/mongo-driver/x/mongo/driver"

// pinnedServer returns the server a request must be routed to, if any. A transaction pin is only honoured for
// statements continuing a transaction: a new transaction (startTransaction) must select a fresh server, as the
// pin may be stale (e.g. after a replica set failover). Otherwise a cursor pin is consulted.
func pinnedServer(
	txns *transactionCache,
	cursors *cursorCache,
	td *TransactionDetails,
	cursorID int64,
	ns string,
) (driver.Server, bool) {
	if td != nil && !td.IsStartTransaction {
		if server, ok := txns.peek(td.LsID); ok {
			return server, true
		}
	}
	if cursorID != 0 {
		if server, ok := cursors.peek(cursorID, ns); ok {
			return server, true
		}
	}
	return nil, false
}
