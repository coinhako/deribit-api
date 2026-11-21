package deribit

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// refusingDeribit answers every WebSocket handshake with HTTP 503, as Deribit's edge does during a
// maintenance, and counts the dial attempts.
func refusingDeribit(dials *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(dials, 1)
		http.Error(w, "error code: 503", http.StatusServiceUnavailable)
	}))
}

// answeringDeribit accepts the WebSocket after handshakeDelay and answers every JSON-RPC call.
func answeringDeribit(handshakeDelay time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(handshakeDelay)
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		for {
			var call struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if err := wsjson.Read(context.Background(), conn, &call); err != nil {
				return
			}
			var result interface{} = "ok"
			if call.Method == "public/test" {
				result = map[string]string{"version": "1.2.26"}
			}
			reply := map[string]interface{}{"jsonrpc": "2.0", "id": call.ID, "result": result}
			if err := wsjson.Write(context.Background(), conn, reply); err != nil {
				return
			}
		}
	}))
}

func TestNewWithContextStopsDialingWhenTheContextEnds(t *testing.T) {
	var dials int32
	srv := refusingDeribit(&dials)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)

	started := time.Now()
	client, err := NewWithContext(ctx, &Configuration{Addr: wsURL(srv)})

	assert.Nil(t, client)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
	assert.Less(t, int64(time.Since(started)), int64(2*time.Second), "the first retry sleeps 5 s, so the context must cut it short")
	assert.EqualValues(t, 1, atomic.LoadInt32(&dials))
}

func TestNewWithContextDoesNotDialWithAContextThatAlreadyEnded(t *testing.T) {
	var dials int32
	srv := refusingDeribit(&dials)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client, err := NewWithContext(ctx, &Configuration{Addr: wsURL(srv)})

	assert.Nil(t, client)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
	assert.EqualValues(t, 0, atomic.LoadInt32(&dials))
}

func TestNewWithContextReturnsAClientThatOutlivesTheContext(t *testing.T) {
	srv := answeringDeribit(0)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())

	client, err := NewWithContext(ctx, &Configuration{Addr: wsURL(srv)})
	require.NoError(t, err)
	defer client.Close()
	cancel()

	assert.True(t, client.IsConnected())
	_, err = client.Test()
	assert.NoError(t, err, "the context bounds connecting only, so calls still work after it ends")
}

func TestNewWithContextDropsAConnectionThatLandsAfterTheContextEnds(t *testing.T) {
	srv := answeringDeribit(300 * time.Millisecond)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	client, err := NewWithContext(ctx, &Configuration{Addr: wsURL(srv)})

	assert.Nil(t, client)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
}
