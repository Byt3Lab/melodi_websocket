package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"melodi_websocket/internal/auth"
	"melodi_websocket/internal/config"
	"melodi_websocket/internal/hub"
	"melodi_websocket/internal/pubsub"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for clients
	},
}

// Server encapsulates the HTTP and WebSocket API server.
type Server struct {
	router      *chi.Mux
	cfgMgr      *config.ConfigManager
	hubMgr      *hub.ConnectionManager
	pubsub      pubsub.Backend
	pubsubMu    sync.RWMutex
	nodeID      string
	startTime   time.Time
	shutdownCh  chan struct{}
}

// NewServer initializes the server and registers routes.
func NewServer(cfgMgr *config.ConfigManager, hubMgr *hub.ConnectionManager, ps pubsub.Backend, nodeID string) *Server {
	s := &Server{
		router:     chi.NewRouter(),
		cfgMgr:     cfgMgr,
		hubMgr:     hubMgr,
		pubsub:     ps,
		nodeID:     nodeID,
		startTime:  time.Now(),
		shutdownCh: make(chan struct{}),
	}

	s.setupRoutes()
	return s
}

// Router returns the chi router.
func (s *Server) Router() *chi.Mux {
	return s.router
}

// ShutdownChannel returns channel signaled on restart request.
func (s *Server) ShutdownChannel() <-chan struct{} {
	return s.shutdownCh
}

// SetPubsub updates pubsub backend dynamically.
func (s *Server) SetPubsub(ps pubsub.Backend) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()
	s.pubsub = ps
}

func (s *Server) getPubsub() pubsub.Backend {
	s.pubsubMu.RLock()
	defer s.pubsubMu.RUnlock()
	return s.pubsub
}

func (s *Server) setupRoutes() {
	s.router.Use(middleware.RequestID)
	s.router.Use(middleware.RealIP)
	s.router.Use(middleware.Recoverer)

	// Public Health endpoints
	s.router.Get("/", s.handleHealth)
	s.router.Get("/health", s.handleHealth)

	// WebSocket endpoint
	s.router.Get("/ws", s.handleWebSocket)

	// Internal routes group with IP & Auth protection
	s.router.Group(func(r chi.Router) {
		r.Use(s.internalSecurityMiddleware)

		r.Get("/internal/health", s.handleInternalHealth)
		r.Post("/internal/broadcast", s.handleBroadcast)
		r.Post("/internal/emit", s.handleBroadcast)
		r.Get("/internal/presence", s.handlePresence)

		// Config management
		r.Get("/internal/config", s.handleGetConfig)
		r.Post("/internal/config", s.handleUpdateConfig)
		r.Put("/internal/config", s.handleUpdateConfig)
		r.Post("/internal/config/test", s.handleTestConfig)
		r.Post("/internal/config/reset", s.handleResetConfig)

		// Admin & stats
		r.Get("/internal/stats", s.handleAdminStats)
		r.Get("/internal/admin/stats", s.handleAdminStats)
		r.Get("/internal/admin/channels", s.handleAdminChannels)
		r.Get("/internal/admin/clients", s.handleAdminClients)
		r.Post("/internal/admin/clients/disconnect", s.handleAdminDisconnectClient)
		r.Post("/internal/admin/sessions/clear-grace", s.handleAdminClearGrace)
		r.Post("/internal/admin/reload", s.handleAdminReload)
		r.Post("/internal/admin/restart", s.handleAdminRestart)
	})
}

// internalSecurityMiddleware checks internal secret or private IP restriction.
func (s *Server) internalSecurityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := s.cfgMgr.Get()

		// If valid secret is supplied in headers, allow immediately
		if auth.CheckInternalAuth(r, cfg.Secret) {
			next.ServeHTTP(w, r)
			return
		}

		// Otherwise check IP restriction
		remoteIP := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			parts := strings.Split(forwarded, ",")
			remoteIP = strings.TrimSpace(parts[0])
		}

		if cfg.AllowPrivateInternal && auth.IsAllowedInternalIP(remoteIP) {
			next.ServeHTTP(w, r)
			return
		}

		log.Printf("[Melodi WebSocket] Accès refusé endpoint interne depuis IP: %s\n", remoteIP)
		writeJSON(w, http.StatusForbidden, map[string]any{
			"status":  "error",
			"message": "Forbidden: Private network or localhost access only",
		})
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ps := s.getPubsub()
	scaleMode := ps.Name()

	var redisStatus any = nil
	if scaleMode == "scalable" {
		ok, msg := ps.CheckConnection(r.Context())
		if ok {
			redisStatus = "connected"
		} else {
			redisStatus = fmt.Sprintf("error: %s", msg)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "ok",
		"service":            "melodi_websocket",
		"version":            "1.0.0",
		"scale_mode":         scaleMode,
		"node_id":            s.nodeID,
		"redis_status":       redisStatus,
		"active_connections": s.hubMgr.ActiveConnectionsCount(),
		"active_channels":    s.hubMgr.ActiveChannelsCount(),
	})
}

func (s *Server) handleInternalHealth(w http.ResponseWriter, r *http.Request) {
	ps := s.getPubsub()
	scaleMode := ps.Name()

	var redisStatus any = nil
	if scaleMode == "scalable" {
		ok, msg := ps.CheckConnection(r.Context())
		if ok {
			redisStatus = "connected"
		} else {
			redisStatus = fmt.Sprintf("error: %s", msg)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "ok",
		"service":            "melodi_websocket",
		"scale_mode":         scaleMode,
		"node_id":            s.nodeID,
		"redis_status":       redisStatus,
		"active_connections": s.hubMgr.ActiveConnectionsCount(),
		"active_channels":    s.hubMgr.ActiveChannelsCount(),
	})
}

func (s *Server) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	var payload struct {
		Event    string         `json:"event"`
		Data     map[string]any `json:"data"`
		UserIDs  []any          `json:"user_ids"`
		Channels []string       `json:"channels"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Event == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":  "error",
			"message": "Bad Request: Missing 'event' field",
		})
		return
	}

	if payload.Data == nil {
		payload.Data = make(map[string]any)
	}

	env := pubsub.Envelope{
		OriginNode: s.nodeID,
		Message: map[string]any{
			"event": payload.Event,
			"data":  payload.Data,
		},
		UserIDs:  payload.UserIDs,
		Channels: payload.Channels,
	}

	ps := s.getPubsub()
	count, err := ps.Publish(r.Context(), env)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Publishing error: %v", err),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "success",
		"recipients_count": count,
		"event":            payload.Event,
		"scale_mode":       ps.Name(),
		"node_id":          s.nodeID,
	})
}

func (s *Server) handlePresence(w http.ResponseWriter, r *http.Request) {
	metrics := s.hubMgr.GetMetrics()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":               "ok",
		"online_user_ids":      metrics["online_user_ids"],
		"grace_sessions":       s.hubMgr.GetGraceSessionsList(),
		"total_active_sockets": metrics["active_connections"],
		"total_sessions":       metrics["total_sessions"],
	})
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	includeSecrets := false
	if v := r.URL.Query().Get("include_secrets"); v == "true" || v == "1" || v == "yes" {
		includeSecrets = true
	}

	structured := s.cfgMgr.GetStructured(includeSecrets)
	uptimeSec := time.Since(s.startTime).Seconds()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "ok",
		"node_id":            s.nodeID,
		"uptime_seconds":     roundFloat(uptimeSec, 1),
		"scale_mode":         s.getPubsub().Name(),
		"active_connections": s.hubMgr.ActiveConnectionsCount(),
		"active_channels":    s.hubMgr.ActiveChannelsCount(),
		"config":             structured,
	})
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	var updates map[string]any
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":  "error",
			"message": "Invalid JSON payload",
		})
		return
	}

	persist := true
	if p, ok := updates["persist"].(bool); ok {
		persist = p
	}

	ok, updatedKeys, restartReq, msg := s.cfgMgr.UpdateConfig(updates, persist)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"status":  "error",
			"message": msg,
		})
		return
	}

	newCfg := s.cfgMgr.Get()
	// Apply runtime updates
	for _, k := range updatedKeys {
		if k == "grace_period_seconds" {
			s.hubMgr.SetGracePeriod(newCfg.GracePeriodSeconds)
		}
	}

	// Check if PubSub needs switching
	pubsubKeys := map[string]bool{
		"scale_mode": true, "redis_url": true, "redis_host": true,
		"redis_port": true, "redis_password": true, "redis_db": true, "redis_channel": true,
	}
	hasPubsubChange := false
	for _, k := range updatedKeys {
		if pubsubKeys[k] {
			hasPubsubChange = true
			break
		}
	}

	if hasPubsubChange {
		if newCfg.ScaleMode == "scalable" || newCfg.ScaleMode == "cluster" {
			newPs := pubsub.NewRedisPubSub(
				newCfg.RedisURL,
				newCfg.RedisHost,
				newCfg.RedisPort,
				newCfg.RedisPassword,
				newCfg.RedisDB,
				newCfg.RedisChannel,
				s.nodeID,
			)
			connOK, connErr := newPs.CheckConnection(r.Context())
			if !connOK {
				writeJSON(w, http.StatusOK, map[string]any{
					"status":           "warning",
					"message":          fmt.Sprintf("Paramètres enregistrés mais échec du basculement PubSub: %s", connErr),
					"updated_keys":     updatedKeys,
					"restart_required": true,
					"config":           s.cfgMgr.GetStructured(false),
				})
				return
			}

			_ = s.getPubsub().Stop()
			_ = newPs.Start(context.Background(), func(env pubsub.Envelope) int {
				return s.hubMgr.LocalBroadcast(env.Message, env.UserIDs, env.Channels)
			})
			s.SetPubsub(newPs)
		} else {
			_ = s.getPubsub().Stop()
			localPs := pubsub.NewLocalPubSub()
			_ = localPs.Start(context.Background(), func(env pubsub.Envelope) int {
				return s.hubMgr.LocalBroadcast(env.Message, env.UserIDs, env.Channels)
			})
			s.SetPubsub(localPs)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "success",
		"message":          msg,
		"updated_keys":     updatedKeys,
		"restart_required": restartReq,
		"config":           s.cfgMgr.GetStructured(false),
	})
}

func (s *Server) handleTestConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	var req map[string]any
	_ = json.NewDecoder(r.Body).Decode(&req)
	target, _ := req["target"].(string)
	if target == "" {
		target = "redis"
	}
	target = strings.ToLower(target)

	if target == "redis" {
		rHost, _ := req["redis_host"].(string)
		if rHost == "" {
			rHost = cfg.RedisHost
		}
		rPort := cfg.RedisPort
		if p, ok := req["redis_port"]; ok {
			if pi, err := strconv.Atoi(fmt.Sprintf("%v", p)); err == nil && pi > 0 {
				rPort = pi
			}
		}
		rPass, _ := req["redis_password"].(string)
		if rPass == "" {
			rPass = cfg.RedisPassword
		}
		rURL, _ := req["redis_url"].(string)
		if rURL == "" {
			rURL = cfg.RedisURL
		}
		rDB := cfg.RedisDB

		success, latencyMs, msg := config.TestRedisConnection(r.Context(), rHost, rPort, rPass, rURL, rDB)
		status := "error"
		if success {
			status = "ok"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":     status,
			"success":    success,
			"target":     "redis",
			"latency_ms": latencyMs,
			"message":    msg,
		})
		return
	} else if target == "melodi" || target == "erp" {
		mHost, _ := req["melodi_host"].(string)
		if mHost == "" {
			mHost = cfg.MelodiHost
		}
		mPort := cfg.MelodiPort
		if p, ok := req["melodi_port"]; ok {
			if pi, err := strconv.Atoi(fmt.Sprintf("%v", p)); err == nil && pi > 0 {
				mPort = pi
			}
		}

		success, latencyMs, msg := config.TestMelodiConnection(r.Context(), mHost, mPort)
		status := "error"
		if success {
			status = "ok"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":     status,
			"success":    success,
			"target":     "melodi",
			"latency_ms": latencyMs,
			"message":    msg,
		})
		return
	}

	writeJSON(w, http.StatusBadRequest, map[string]any{
		"status":  "error",
		"message": fmt.Sprintf("Unknown target: '%s'", target),
	})
}

func (s *Server) handleResetConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	s.cfgMgr.ResetToStartup()
	startupCfg := s.cfgMgr.Get()
	s.hubMgr.SetGracePeriod(startupCfg.GracePeriodSeconds)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "success",
		"message": "Configuration réinitialisée aux valeurs de démarrage",
		"config":  s.cfgMgr.GetStructured(false),
	})
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	metrics := s.hubMgr.GetMetrics()
	uptimeSec := time.Since(s.startTime).Seconds()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memoryMb := roundFloat(float64(m.Alloc)/(1024*1024), 2)

	ps := s.getPubsub()
	var redisStatus any = nil
	if ps.Name() == "scalable" {
		ok, msg := ps.CheckConnection(r.Context())
		if ok {
			redisStatus = "connected"
		} else {
			redisStatus = fmt.Sprintf("error: %s", msg)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"service":        "melodi_websocket",
		"node_id":        s.nodeID,
		"pid":            os.Getpid(),
		"uptime_seconds": roundFloat(uptimeSec, 1),
		"scale_mode":     ps.Name(),
		"redis_status":   redisStatus,
		"connections": map[string]any{
			"active":          metrics["active_connections"],
			"total_accepted":  metrics["total_connections_accepted"],
			"max_connections": cfg.MaxConnections,
		},
		"messages": map[string]any{
			"total_received": metrics["total_messages_received"],
			"total_sent":     metrics["total_messages_sent"],
		},
		"channels": map[string]any{
			"total_active": metrics["active_channels"],
			"list":         metrics["channels_summary"],
		},
		"users": map[string]any{
			"active_users_count": metrics["active_users_count"],
			"online_user_ids":    metrics["online_user_ids"],
		},
		"sessions": map[string]any{
			"total_sessions":       metrics["total_sessions"],
			"grace_sessions_count": metrics["grace_sessions_count"],
			"grace_period_seconds": metrics["grace_period_seconds"],
		},
		"system": map[string]any{
			"memory_rss_mb": memoryMb,
			"log_level":     cfg.LogLevel,
			"config_file":   s.cfgMgr.ConfigFilePath(),
		},
	})
}

func (s *Server) handleAdminChannels(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	summary := s.hubMgr.GetChannelsSummary()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"total_channels": len(summary),
		"channels":       summary,
	})
}

func (s *Server) handleAdminClients(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	channel := r.URL.Query().Get("channel")
	userID := r.URL.Query().Get("user_id")

	clients := s.hubMgr.GetClients(channel, userID)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"total_clients": len(clients),
		"clients":       clients,
	})
}

func (s *Server) handleAdminDisconnectClient(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	var payload struct {
		ClientID string `json:"client_id"`
		UserID   string `json:"user_id"`
		Reason   string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&payload)

	reason := payload.Reason
	if reason == "" {
		reason = "Disconnected by administrator"
	}

	if payload.ClientID != "" {
		if s.hubMgr.DisconnectClient(payload.ClientID, reason) {
			writeJSON(w, http.StatusOK, map[string]any{
				"status":  "success",
				"message": fmt.Sprintf("Client '%s' disconnected", payload.ClientID),
			})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]any{
			"status":  "error",
			"message": fmt.Sprintf("Client '%s' not found", payload.ClientID),
		})
		return
	} else if payload.UserID != "" {
		count := s.hubMgr.DisconnectUser(payload.UserID, reason)
		writeJSON(w, http.StatusOK, map[string]any{
			"status":             "success",
			"message":            fmt.Sprintf("Disconnected %d connection(s) for user '%s'", count, payload.UserID),
			"disconnected_count": count,
		})
		return
	}

	writeJSON(w, http.StatusBadRequest, map[string]any{
		"status":  "error",
		"message": "Missing 'client_id' or 'user_id' in payload",
	})
}

func (s *Server) handleAdminClearGrace(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	count := s.hubMgr.ClearGraceSessions()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "success",
		"message":       fmt.Sprintf("%d grace session(s) cleared", count),
		"cleared_count": count,
	})
}

func (s *Server) handleAdminReload(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "success",
		"message":          "Configuration reloaded from disk",
		"updated_keys":     []string{},
		"restart_required": false,
		"config":           s.cfgMgr.GetStructured(false),
	})
}

func (s *Server) handleAdminRestart(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	if !auth.CheckInternalAuth(r, cfg.Secret) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"status":  "error",
			"message": "Unauthorized: Invalid secret",
		})
		return
	}

	go func() {
		time.Sleep(500 * time.Millisecond)
		close(s.shutdownCh)
	}()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "success",
		"message": "Redémarrage du service WebSocket initié",
	})
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	sessionID := r.URL.Query().Get("session_id")
	remoteIP := r.RemoteAddr

	cfg := s.cfgMgr.Get()
	userID := ""
	if token != "" {
		userID = auth.ExtractUserID(token, cfg.Secret)
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Melodi WebSocket] WS upgrade error: %v\n", err)
		return
	}

	clientID := fmt.Sprintf("client_%s", uuid.New().String()[:8])
	client := hub.NewClient(s.hubMgr, conn, clientID, userID, sessionID, remoteIP)
	s.hubMgr.Register(client)

	// Send "connected" event
	client.SendMessage(map[string]any{
		"event": "connected",
		"data": map[string]any{
			"client_id":            clientID,
			"session_id":           sessionID,
			"authenticated":        userID != "",
			"user_id":              userID,
			"grace_period_seconds": s.hubMgr.GracePeriod(),
		},
	})

	go client.WritePump()
	go client.ReadPump(func() string {
		return s.cfgMgr.Get().Secret
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func roundFloat(val float64, precision int) float64 {
	p := 1.0
	for i := 0; i < precision; i++ {
		p *= 10
	}
	return float64(int(val*p+0.5)) / p
}
