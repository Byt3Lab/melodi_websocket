package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Envelope matches the JSON envelope passed between nodes or in-memory.
type Envelope struct {
	OriginNode string         `json:"origin_node"`
	Message    map[string]any `json:"message"`
	UserIDs    []any          `json:"user_ids,omitempty"`
	Channels   []string       `json:"channels,omitempty"`
}

// MessageHandler is the callback invoked when an envelope arrives.
type MessageHandler func(env Envelope) int

// Backend defines the interface for local or distributed event fanout.
type Backend interface {
	CheckConnection(ctx context.Context) (bool, string)
	Start(ctx context.Context, onMessage MessageHandler) error
	Publish(ctx context.Context, env Envelope) (int, error)
	Stop() error
	Name() string
}

// LocalPubSub is the fast in-memory PubSub backend for autonomous mode.
type LocalPubSub struct {
	mu        sync.RWMutex
	onMessage MessageHandler
}

// NewLocalPubSub initializes a local in-memory pubsub.
func NewLocalPubSub() *LocalPubSub {
	return &LocalPubSub{}
}

func (l *LocalPubSub) Name() string {
	return "autonome"
}

func (l *LocalPubSub) CheckConnection(ctx context.Context) (bool, string) {
	return true, "Mode autonome (in-memory) opérationnel"
}

func (l *LocalPubSub) Start(ctx context.Context, onMessage MessageHandler) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onMessage = onMessage
	log.Println("[PubSub] Initialisé en mode AUTONOME (in-memory).")
	return nil
}

func (l *LocalPubSub) Publish(ctx context.Context, env Envelope) (int, error) {
	l.mu.RLock()
	handler := l.onMessage
	l.mu.RUnlock()

	if handler != nil {
		return handler(env), nil
	}
	return 0, nil
}

func (l *LocalPubSub) Stop() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onMessage = nil
	return nil
}

// RedisPubSub is the distributed backend for scalable / cluster mode.
type RedisPubSub struct {
	mu        sync.RWMutex
	client    *redis.Client
	pubsub    *redis.PubSub
	channel   string
	cancel    context.CancelFunc
	onMessage MessageHandler
	wg        sync.WaitGroup
	nodeID    string
}

// NewRedisPubSub initializes a Redis Pub/Sub backend.
func NewRedisPubSub(redisURL, host string, port int, password string, db int, channel, nodeID string) *RedisPubSub {
	var opt *redis.Options
	if redisURL != "" {
		parsed, err := redis.ParseURL(redisURL)
		if err == nil {
			opt = parsed
		}
	}
	if opt == nil {
		if host == "" {
			host = "localhost"
		}
		if port <= 0 {
			port = 6379
		}
		opt = &redis.Options{
			Addr:     net.JoinHostPort(host, strconv.Itoa(port)),
			Password: password,
			DB:       db,
		}
	}

	if channel == "" {
		channel = "melodi:ws:events"
	}

	client := redis.NewClient(opt)
	return &RedisPubSub{
		client:  client,
		channel: channel,
		nodeID:  nodeID,
	}
}

func (r *RedisPubSub) Name() string {
	return "scalable"
}

func (r *RedisPubSub) CheckConnection(ctx context.Context) (bool, string) {
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	cmd := r.client.Ping(pingCtx)
	if err := cmd.Err(); err != nil {
		return false, fmt.Sprintf("Connexion Redis impossible: %v", err)
	}
	return true, "Connexion Redis opérationnelle"
}

func (r *RedisPubSub) Start(ctx context.Context, onMessage MessageHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onMessage = onMessage
	subCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	r.pubsub = r.client.Subscribe(subCtx, r.channel)
	// Verify subscription
	_, err := r.pubsub.Receive(subCtx)
	if err != nil {
		return fmt.Errorf("échec souscription Redis: %w", err)
	}

	r.wg.Add(1)
	go r.listenLoop(subCtx)

	log.Printf("[PubSub] Initialisé en mode SCALABLE (Redis channel: %s).\n", r.channel)
	return nil
}

func (r *RedisPubSub) listenLoop(ctx context.Context) {
	defer r.wg.Done()
	ch := r.pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var env Envelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				log.Printf("[PubSub] Erreur décodage message Redis: %v\n", err)
				continue
			}

			r.mu.RLock()
			handler := r.onMessage
			r.mu.RUnlock()

			if handler != nil {
				handler(env)
			}
		}
	}
}

func (r *RedisPubSub) Publish(ctx context.Context, env Envelope) (int, error) {
	data, err := json.Marshal(env)
	if err != nil {
		return 0, err
	}

	pubCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	res := r.client.Publish(pubCtx, r.channel, data)
	if err := res.Err(); err != nil {
		return 0, err
	}

	// In scalable mode, also dispatch locally for instant feedback if origin node is this one
	r.mu.RLock()
	handler := r.onMessage
	r.mu.RUnlock()

	localCount := 0
	if handler != nil {
		localCount = handler(env)
	}
	return localCount, nil
}

func (r *RedisPubSub) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.cancel != nil {
		r.cancel()
	}
	if r.pubsub != nil {
		_ = r.pubsub.Close()
	}
	r.wg.Wait()
	if r.client != nil {
		_ = r.client.Close()
	}
	return nil
}
