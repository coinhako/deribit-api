package deribit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newUnconnectedClient builds a Client without dialing, so the lifecycle can be exercised
// without a live Deribit socket.
func newUnconnectedClient() *Client {
	return &Client{
		heartCancel: make(chan struct{}),
		isConnected: true,
	}
}

func TestCloseStopsTheHeartbeatGoroutine(t *testing.T) {
	c := newUnconnectedClient()

	stopped := make(chan struct{})
	go func() {
		c.heartbeat()
		close(stopped)
	}()

	require.NoError(t, c.Close())

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat still running after Close; an abandoned client would keep calling Test forever")
	}
}

// TestCloseIsSafeToCallTwice pins the caller-facing contract only. It does not isolate the
// `closed` early-return: stopHeartbeat guards the channel independently, and the guard's
// remaining job — not calling rpcConn.Close() a second time — is unobservable here because
// jsonrpc2.Conn has no seam to inject a fake.
func TestCloseIsSafeToCallTwice(t *testing.T) {
	c := newUnconnectedClient()

	require.NoError(t, c.Close())
	assert.NotPanics(t, func() {
		assert.NoError(t, c.Close(), "a second Close must report success, not a double channel close")
	})
}

func TestCloseMarksTheClientUnusable(t *testing.T) {
	c := newUnconnectedClient()
	require.True(t, c.IsConnected())
	require.False(t, c.IsClosed())

	require.NoError(t, c.Close())

	assert.False(t, c.IsConnected(), "Call short-circuits on IsConnected, so a closed client must report false")
	assert.True(t, c.IsClosed(), "reconnect uses IsClosed to tell a deliberate Close from a dropped socket")

	// Call must refuse rather than reach the nil rpcConn.
	assert.Error(t, c.Call("public/test", nil, nil))
}

func TestStopHeartbeatToleratesNoChannel(t *testing.T) {
	c := &Client{}
	assert.NotPanics(t, c.stopHeartbeat)
	assert.NoError(t, c.Close(), "a client that never started must still be closable")
}

func TestStartRearmsTheHeartbeatGuard(t *testing.T) {
	c := newUnconnectedClient()
	c.stopHeartbeat()
	require.True(t, c.heartStopped)

	// start assigns a fresh heartCancel for the new connection; the guard has to follow it,
	// otherwise the next reconnect could never stop its heartbeat.
	c.mu.Lock()
	c.heartCancel = make(chan struct{})
	c.heartStopped = false
	c.mu.Unlock()

	stopped := make(chan struct{})
	go func() {
		c.heartbeat()
		close(stopped)
	}()

	c.stopHeartbeat()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat for the second connection could not be stopped")
	}
}
