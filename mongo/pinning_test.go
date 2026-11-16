package mongo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPinnedServerStartTransactionIgnoresStalePin(t *testing.T) {
	txns, cursors := newTransactionCache(), newCursorCache()
	txns.add([]byte{0x1}, &transactionMockServer{1})

	s, ok := pinnedServer(txns, cursors, &TransactionDetails{LsID: []byte{0x1}, IsStartTransaction: true}, 0, "")
	assert.False(t, ok)
	assert.Nil(t, s)
}

func TestPinnedServerContinuesTransaction(t *testing.T) {
	txns, cursors := newTransactionCache(), newCursorCache()
	a := &transactionMockServer{1}
	txns.add([]byte{0x1}, a)

	s, ok := pinnedServer(txns, cursors, &TransactionDetails{LsID: []byte{0x1}}, 0, "")
	assert.True(t, ok)
	assert.Same(t, a, s)
}

func TestPinnedServerCursor(t *testing.T) {
	txns, cursors := newTransactionCache(), newCursorCache()
	b := &transactionMockServer{2}
	cursors.add(5, "test.c", b)

	s, ok := pinnedServer(txns, cursors, nil, 5, "test.c")
	assert.True(t, ok)
	assert.Same(t, b, s)
}
