package hub

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"melodi_websocket/internal/pubsub"
)

// SessionData retains disconnected client context during grace period.
type SessionData struct {
	UserID       string
	Channels     map[string]bool
	Timer        *time.Timer
	DisconnectAt time.Time
}

// ConnectionManager manages all connected clients, subscriptions, sessions, and metrics.
type ConnectionManager struct {
	mu                       sync.RWMutex
	activeClients            map[*Client]bool
	userSockets              map[string]map[*Client]bool
	channelSockets           map[string]map[*Client]bool
	sessions                 map[string]*SessionData
	gracePeriodSeconds       int
	nodeID                   string
	pubsubPublisher          func(ctx context.Context, env pubsub.Envelope) (int, error)
	totalConnectionsAccepted int64
	totalMessagesReceived    int64
	totalMessagesSent        int64
}

// NewConnectionManager initializes the hub manager.
func NewConnectionManager(gracePeriodSeconds int, nodeID string) *ConnectionManager {
	if gracePeriodSeconds <= 0 {
		gracePeriodSeconds = 180
	}
	return &ConnectionManager{
		activeClients:      make(map[*Client]bool),
		userSockets:        make(map[string]map[*Client]bool),
		channelSockets:     make(map[string]map[*Client]bool),
		sessions:           make(map[string]*SessionData),
		gracePeriodSeconds: gracePeriodSeconds,
		nodeID:             nodeID,
	}
}

// SetPubsubPublisher assigns the publisher used for offline presence broadcast.
func (cm *ConnectionManager) SetPubsubPublisher(fn func(ctx context.Context, env pubsub.Envelope) (int, error)) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.pubsubPublisher = fn
}

// SetGracePeriod updates grace period duration dynamically.
func (cm *ConnectionManager) SetGracePeriod(sec int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.gracePeriodSeconds = sec
}

// GracePeriod returns current grace period.
func (cm *ConnectionManager) GracePeriod() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.gracePeriodSeconds
}

// IncrementMessagesReceived atomically increments the metric.
func (cm *ConnectionManager) IncrementMessagesReceived() {
	atomic.AddInt64(&cm.totalMessagesReceived, 1)
}

// IncrementMessagesSent atomically increments the metric.
func (cm *ConnectionManager) IncrementMessagesSent() {
	atomic.AddInt64(&cm.totalMessagesSent, 1)
}

// Register registers a newly connected client.
func (cm *ConnectionManager) Register(c *Client) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.activeClients[c] = true
	atomic.AddInt64(&cm.totalConnectionsAccepted, 1)

	uid := c.UserID()
	if uid != "" {
		if cm.userSockets[uid] == nil {
			cm.userSockets[uid] = make(map[*Client]bool)
		}
		cm.userSockets[uid][c] = true
	}

	sid := c.SessionID()
	if sid != "" {
		if sess, exists := cm.sessions[sid]; exists {
			// Resumed session within grace period
			if sess.Timer != nil {
				sess.Timer.Stop()
				sess.Timer = nil
			}
			log.Printf("[Melodi WebSocket] Session %s reprise dans la période de grâce (%ds).\n", sid, cm.gracePeriodSeconds)

			// Restore subscriptions
			for ch := range sess.Channels {
				c.channels[ch] = true
				if cm.channelSockets[ch] == nil {
					cm.channelSockets[ch] = make(map[*Client]bool)
				}
				cm.channelSockets[ch][c] = true
			}

			if uid != "" {
				sess.UserID = uid
			}
		} else {
			cm.sessions[sid] = &SessionData{
				UserID:   uid,
				Channels: make(map[string]bool),
			}
		}
	}

	log.Printf("[Melodi WebSocket] Client registered: %s (user: %s, session: %s, ip: %s)\n", c.ID(), uid, sid, c.RemoteIP())
}

// Reauthenticate updates user and session mapping after in-band 'auth' action.
func (cm *ConnectionManager) Reauthenticate(c *Client, newUserID, sessionID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	oldUID := c.UserID()
	if oldUID != "" && cm.userSockets[oldUID] != nil {
		delete(cm.userSockets[oldUID], c)
		if len(cm.userSockets[oldUID]) == 0 {
			delete(cm.userSockets, oldUID)
		}
	}

	c.mu.Lock()
	c.userID = newUserID
	if sessionID != "" {
		c.sessionID = sessionID
	}
	c.mu.Unlock()

	if newUserID != "" {
		if cm.userSockets[newUserID] == nil {
			cm.userSockets[newUserID] = make(map[*Client]bool)
		}
		cm.userSockets[newUserID][c] = true
	}

	if sessionID != "" {
		if sess, exists := cm.sessions[sessionID]; exists {
			sess.UserID = newUserID
		} else {
			cm.sessions[sessionID] = &SessionData{
				UserID:   newUserID,
				Channels: make(map[string]bool),
			}
		}
	}
}

// Subscribe subscribes client to a channel.
func (cm *ConnectionManager) Subscribe(c *Client, channel string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c.mu.Lock()
	c.channels[channel] = true
	sid := c.sessionID
	c.mu.Unlock()

	if cm.channelSockets[channel] == nil {
		cm.channelSockets[channel] = make(map[*Client]bool)
	}
	cm.channelSockets[channel][c] = true

	if sid != "" && cm.sessions[sid] != nil {
		cm.sessions[sid].Channels[channel] = true
	}

	log.Printf("[Melodi WebSocket] Client %s subscribed to '%s'\n", c.ID(), channel)
}

// Unsubscribe unsubscribes client from a channel.
func (cm *ConnectionManager) Unsubscribe(c *Client, channel string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	c.mu.Lock()
	delete(c.channels, channel)
	sid := c.sessionID
	c.mu.Unlock()

	if cm.channelSockets[channel] != nil {
		delete(cm.channelSockets[channel], c)
		if len(cm.channelSockets[channel]) == 0 {
			delete(cm.channelSockets, channel)
		}
	}

	if sid != "" && cm.sessions[sid] != nil {
		delete(cm.sessions[sid].Channels, channel)
	}
}

// Unregister cleans up client sockets and starts grace timer if session exists.
func (cm *ConnectionManager) Unregister(c *Client) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.activeClients[c] {
		return
	}
	delete(cm.activeClients, c)

	uid := c.UserID()
	if uid != "" && cm.userSockets[uid] != nil {
		delete(cm.userSockets[uid], c)
		if len(cm.userSockets[uid]) == 0 {
			delete(cm.userSockets, uid)
		}
	}

	c.mu.RLock()
	channels := make(map[string]bool, len(c.channels))
	for ch := range c.channels {
		channels[ch] = true
	}
	sid := c.sessionID
	c.mu.RUnlock()

	for ch := range channels {
		if cm.channelSockets[ch] != nil {
			delete(cm.channelSockets[ch], c)
			if len(cm.channelSockets[ch]) == 0 {
				delete(cm.channelSockets, ch)
			}
		}
	}

	if sid != "" && cm.sessions[sid] != nil {
		sess := cm.sessions[sid]
		sess.Channels = channels
		sess.DisconnectAt = time.Now()

		graceSec := cm.gracePeriodSeconds
		sess.Timer = time.AfterFunc(time.Duration(graceSec)*time.Second, func() {
			cm.expireSession(sid, uid)
		})
		log.Printf("[Melodi WebSocket] Déconnexion socket %s. Période de grâce de %ds activée pour session %s.\n", c.ID(), graceSec, sid)
	} else {
		log.Printf("[Melodi WebSocket] Client unregistered: %s\n", c.ID())
	}
}

func (cm *ConnectionManager) expireSession(sid, uid string) {
	cm.mu.Lock()
	if sess, exists := cm.sessions[sid]; exists {
		delete(cm.sessions, sid)
		_ = sess
		log.Printf("[Melodi WebSocket] Session %s (user: %s) expirée après période de grâce. Déconnexion définitive.\n", sid, uid)

		// If user has no remaining active sockets, publish offline event
		if uid != "" && (cm.userSockets[uid] == nil || len(cm.userSockets[uid]) == 0) {
			publisher := cm.pubsubPublisher
			nodeID := cm.nodeID
			cm.mu.Unlock()

			if publisher != nil {
				env := pubsub.Envelope{
					OriginNode: nodeID,
					Message: map[string]any{
						"event": "user_presence",
						"data": map[string]any{
							"user_id": uid,
							"status":  "offline",
						},
					},
					Channels: []string{"presence", "chat:presence"},
				}
				_, _ = publisher(context.Background(), env)
			}
			return
		}
	}
	cm.mu.Unlock()
}

// LocalBroadcast fans out message to targeted or all local clients.
func (cm *ConnectionManager) LocalBroadcast(message map[string]any, userIDs []any, channels []string) int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	targets := make(map[*Client]bool)

	if len(userIDs) == 0 && len(channels) == 0 {
		for c := range cm.activeClients {
			targets[c] = true
		}
	} else {
		for _, rawUID := range userIDs {
			uidStr := fmt.Sprintf("%v", rawUID)
			if sockets, exists := cm.userSockets[uidStr]; exists {
				for c := range sockets {
					targets[c] = true
				}
			}
		}

		for _, ch := range channels {
			if sockets, exists := cm.channelSockets[ch]; exists {
				for c := range sockets {
					targets[c] = true
				}
			}
		}
	}

	count := 0
	for c := range targets {
		if c.SendMessage(message) {
			count++
		}
	}
	return count
}

// GetClients returns list of active clients, optionally filtered.
func (cm *ConnectionManager) GetClients(channelFilter, userIDFilter string) []map[string]any {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	now := time.Now()
	res := make([]map[string]any, 0, len(cm.activeClients))

	for c := range cm.activeClients {
		if channelFilter != "" {
			c.mu.RLock()
			hasCh := c.channels[channelFilter]
			c.mu.RUnlock()
			if !hasCh {
				continue
			}
		}

		if userIDFilter != "" && c.UserID() != userIDFilter {
			continue
		}

		chans := c.GetChannels()
		sort.Strings(chans)

		res = append(res, map[string]any{
			"client_id":      c.ID(),
			"user_id":        c.UserID(),
			"session_id":     c.SessionID(),
			"channels":       chans,
			"remote_ip":      c.RemoteIP(),
			"connected_at":   c.ConnectedAt().UTC().Format(time.RFC3339),
			"uptime_seconds": roundFloat(now.Sub(c.ConnectedAt()).Seconds(), 1),
		})
	}

	return res
}

// GetChannelsSummary returns all active channels with count of subscribers.
func (cm *ConnectionManager) GetChannelsSummary() []map[string]any {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	summary := make([]map[string]any, 0, len(cm.channelSockets))
	for ch, sockets := range cm.channelSockets {
		summary = append(summary, map[string]any{
			"name":             ch,
			"subscriber_count": len(sockets),
		})
	}

	sort.Slice(summary, func(i, j int) bool {
		return summary[i]["name"].(string) < summary[j]["name"].(string)
	})
	return summary
}

// DisconnectClient forcefully closes client by ID.
func (cm *ConnectionManager) DisconnectClient(clientID, reason string) bool {
	cm.mu.RLock()
	var target *Client
	for c := range cm.activeClients {
		if c.ID() == clientID {
			target = c
			break
		}
	}
	cm.mu.RUnlock()

	if target == nil {
		return false
	}

	target.SendMessage(map[string]any{
		"event": "disconnected",
		"data":  map[string]string{"reason": reason},
	})
	_ = target.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason))
	target.Close()
	return true
}

// DisconnectUser disconnects all sockets belonging to a user ID.
func (cm *ConnectionManager) DisconnectUser(userID, reason string) int {
	cm.mu.RLock()
	sockets := make([]*Client, 0)
	if s, exists := cm.userSockets[userID]; exists {
		for c := range s {
			sockets = append(sockets, c)
		}
	}
	cm.mu.RUnlock()

	count := 0
	for _, c := range sockets {
		c.SendMessage(map[string]any{
			"event": "disconnected",
			"data":  map[string]string{"reason": reason},
		})
		_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason))
		c.Close()
		count++
	}
	return count
}

// ClearGraceSessions purges all sessions waiting in grace period.
func (cm *ConnectionManager) ClearGraceSessions() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	count := 0
	for sid, sess := range cm.sessions {
		if sess.Timer != nil {
			sess.Timer.Stop()
			count++
		}
		delete(cm.sessions, sid)
	}
	return count
}

// GetMetrics returns real-time metrics for admin dashboard.
func (cm *ConnectionManager) GetMetrics() map[string]any {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	graceCount := 0
	for _, s := range cm.sessions {
		if s.Timer != nil {
			graceCount++
		}
	}

	onlineUsers := make([]string, 0, len(cm.userSockets))
	for uid := range cm.userSockets {
		onlineUsers = append(onlineUsers, uid)
	}
	sort.Strings(onlineUsers)

	return map[string]any{
		"active_connections":         len(cm.activeClients),
		"total_connections_accepted": atomic.LoadInt64(&cm.totalConnectionsAccepted),
		"total_messages_received":    atomic.LoadInt64(&cm.totalMessagesReceived),
		"total_messages_sent":        atomic.LoadInt64(&cm.totalMessagesSent),
		"active_channels":            len(cm.channelSockets),
		"channels_summary":           cm.GetChannelsSummary(),
		"active_users_count":         len(cm.userSockets),
		"online_user_ids":            onlineUsers,
		"total_sessions":             len(cm.sessions),
		"grace_sessions_count":       graceCount,
		"grace_period_seconds":       cm.gracePeriodSeconds,
	}
}

// ActiveConnectionsCount returns number of connected sockets.
func (cm *ConnectionManager) ActiveConnectionsCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.activeClients)
}

// ActiveChannelsCount returns number of active channels.
func (cm *ConnectionManager) ActiveChannelsCount() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.channelSockets)
}

// GetGraceSessionsList returns session IDs currently in grace.
func (cm *ConnectionManager) GetGraceSessionsList() []map[string]any {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	res := make([]map[string]any, 0)
	for sid, sess := range cm.sessions {
		if sess.Timer != nil {
			res = append(res, map[string]any{
				"session_id": sid,
				"user_id":    sess.UserID,
			})
		}
	}
	return res
}

func roundFloat(val float64, precision int) float64 {
	p := 1.0
	for i := 0; i < precision; i++ {
		p *= 10
	}
	return float64(int(val*p+0.5)) / p
}
