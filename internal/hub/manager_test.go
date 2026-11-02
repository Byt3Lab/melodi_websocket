package hub_test

import (
	"context"
	"testing"

	"melodi_websocket/internal/hub"
	"melodi_websocket/internal/pubsub"
)

func TestConnectionManagerBasic(t *testing.T) {
	mgr := hub.NewConnectionManager(60, "node_test_hub")

	metrics := mgr.GetMetrics()
	if metrics["active_connections"] != 0 {
		t.Errorf("expected 0 active connections, got %v", metrics["active_connections"])
	}

	if mgr.ActiveConnectionsCount() != 0 {
		t.Errorf("expected 0 active connections count, got %d", mgr.ActiveConnectionsCount())
	}

	if mgr.GracePeriod() != 60 {
		t.Errorf("expected grace period 60, got %d", mgr.GracePeriod())
	}

	mgr.SetGracePeriod(150)
	if mgr.GracePeriod() != 150 {
		t.Errorf("expected grace period 150, got %d", mgr.GracePeriod())
	}

	// Test broadcast with 0 connections returns 0
	count := mgr.LocalBroadcast(map[string]any{"event": "ping"}, nil, []string{"test_channel"})
	if count != 0 {
		t.Errorf("expected 0 recipients, got %d", count)
	}

	// Test set pubsub publisher
	called := false
	mgr.SetPubsubPublisher(func(ctx context.Context, env pubsub.Envelope) (int, error) {
		called = true
		return 1, nil
	})
	_ = called
}
