package hub

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"melodi_websocket/internal/auth"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512 * 1024 // 512 KB
)

// Client represents a connected WebSocket peer.
type Client struct {
	hub         *ConnectionManager
	conn        *websocket.Conn
	send        chan []byte
	mu          sync.RWMutex
	id          string
	userID      string
	sessionID   string
	remoteIP    string
	connectedAt time.Time
	channels    map[string]bool
	closed      int32
}

// NewClient initializes a client struct.
func NewClient(hub *ConnectionManager, conn *websocket.Conn, id, userID, sessionID, remoteIP string) *Client {
	return &Client{
		hub:         hub,
		conn:        conn,
		send:        make(chan []byte, 256),
		id:          id,
		userID:      userID,
		sessionID:   sessionID,
		remoteIP:    remoteIP,
		connectedAt: time.Now(),
		channels:    make(map[string]bool),
	}
}

// ID returns client identifier.
func (c *Client) ID() string {
	return c.id
}

// UserID returns client's user identifier.
func (c *Client) UserID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userID
}

// SessionID returns the session ID.
func (c *Client) SessionID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionID
}

// RemoteIP returns the client's IP.
func (c *Client) RemoteIP() string {
	return c.remoteIP
}

// ConnectedAt returns connection time.
func (c *Client) ConnectedAt() time.Time {
	return c.connectedAt
}

// GetChannels returns a copy of current channel subscriptions.
func (c *Client) GetChannels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	list := make([]string, 0, len(c.channels))
	for ch := range c.channels {
		list = append(list, ch)
	}
	return list
}

// SendMessage safely queues a JSON message to this client.
func (c *Client) SendMessage(msg any) bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return false
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return false
	}

	select {
	case c.send <- data:
		c.hub.IncrementMessagesSent()
		return true
	default:
		// Send buffer full, drop or close
		return false
	}
}

// Close gracefully closes the client connection.
func (c *Client) Close() {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		close(c.send)
		_ = c.conn.Close()
	}
}

// ReadPump listens for messages from the WebSocket client.
func (c *Client) ReadPump(secretProvider func() string) {
	defer func() {
		c.hub.Unregister(c)
		c.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				log.Printf("[Melodi WebSocket] Read error on %s: %v\n", c.id, err)
			}
			break
		}

		c.hub.IncrementMessagesReceived()

		var req map[string]any
		if err := json.Unmarshal(message, &req); err != nil {
			c.SendMessage(map[string]any{
				"event": "error",
				"data":  map[string]string{"message": "Invalid JSON format"},
			})
			continue
		}

		action, _ := req["action"].(string)
		if action == "" {
			action, _ = req["type"].(string)
		}

		switch action {
		case "ping":
			c.SendMessage(map[string]any{
				"event": "pong",
				"data":  map[string]any{},
			})

		case "auth":
			tokenStr, _ := req["token"].(string)
			sid, _ := req["session_id"].(string)
			if sid == "" {
				sid = c.SessionID()
			}

			secret := secretProvider()
			newUID := auth.ExtractUserID(tokenStr, secret)
			if newUID != "" {
				c.hub.Reauthenticate(c, newUID, sid)
				c.SendMessage(map[string]any{
					"event": "authenticated",
					"data": map[string]any{
						"user_id":    newUID,
						"session_id": sid,
					},
				})
			} else {
				c.SendMessage(map[string]any{
					"event": "error",
					"data":  map[string]string{"message": "Authentication failed: invalid token"},
				})
			}

		case "subscribe":
			ch, _ := req["channel"].(string)
			if ch != "" {
				c.hub.Subscribe(c, ch)
				c.SendMessage(map[string]any{
					"event": "subscribed",
					"data":  map[string]string{"channel": ch},
				})
			}

		case "unsubscribe":
			ch, _ := req["channel"].(string)
			if ch != "" {
				c.hub.Unsubscribe(c, ch)
				c.SendMessage(map[string]any{
					"event": "unsubscribed",
					"data":  map[string]string{"channel": ch},
				})
			}

		default:
			c.SendMessage(map[string]any{
				"event": "info",
				"data":  map[string]string{"message": "For state modifications, please use the main REST API."},
			})
		}
	}
}

// WritePump pumps messages from the send channel to the WebSocket connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(msg)

			// Optimize: flush any queued messages in the channel
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte{'\n'})
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
