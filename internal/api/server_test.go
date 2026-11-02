package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"melodi_websocket/internal/api"
	"melodi_websocket/internal/auth"
	"melodi_websocket/internal/config"
	"melodi_websocket/internal/hub"
	"melodi_websocket/internal/pubsub"
)

func setupTestServer(t *testing.T) (*httptest.Server, *api.Server, *config.ConfigManager, *hub.ConnectionManager) {
	tmpDir := t.TempDir()
	cfgMgr := config.NewConfigManager(tmpDir, "config.json")
	cfgMgr.InitConfig(config.FlatConfig{}, nil)

	nodeID := "node_test1234"
	hubMgr := hub.NewConnectionManager(180, nodeID)
	ps := pubsub.NewLocalPubSub()

	_ = ps.Start(context.Background(), func(env pubsub.Envelope) int {
		return hubMgr.LocalBroadcast(env.Message, env.UserIDs, env.Channels)
	})

	server := api.NewServer(cfgMgr, hubMgr, ps, nodeID)
	ts := httptest.NewServer(server.Router())

	return ts, server, cfgMgr, hubMgr
}

func TestRootHealth(t *testing.T) {
	ts, _, _, _ := setupTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("failed to GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if data["status"] != "ok" || data["service"] != "melodi_websocket" {
		t.Errorf("unexpected body: %+v", data)
	}
}

func TestInternalHealth(t *testing.T) {
	ts, _, _, _ := setupTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/internal/health", nil)
	req.Header.Set("X-Internal-Secret", "melodi_ws_secret_456")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var data map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&data)
	if data["status"] != "ok" {
		t.Errorf("expected ok status, got %v", data["status"])
	}
}

func TestBroadcastUnauthorized(t *testing.T) {
	ts, _, _, _ := setupTestServer(t)
	defer ts.Close()

	body := bytes.NewBufferString(`{"event": "test"}`)
	req, _ := http.NewRequest("POST", ts.URL+"/internal/broadcast", body)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}
}

func TestBroadcastAuthorizedSuccess(t *testing.T) {
	ts, _, _, _ := setupTestServer(t)
	defer ts.Close()

	payload := map[string]any{
		"event": "order_created",
		"data":  map[string]any{"order_id": 42},
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", ts.URL+"/internal/broadcast", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Secret", "melodi_ws_secret_456")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var data map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&data)
	if data["status"] != "success" || data["event"] != "order_created" {
		t.Errorf("unexpected response: %+v", data)
	}
}

func TestConfigMasking(t *testing.T) {
	ts, _, cfgMgr, _ := setupTestServer(t)
	defer ts.Close()

	secret := cfgMgr.Get().Secret

	// 1. Without include_secrets
	req, _ := http.NewRequest("GET", ts.URL+"/internal/config", nil)
	req.Header.Set("X-Internal-Secret", secret)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed request: %v", err)
	}
	defer resp.Body.Close()

	var data map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&data)

	cfgMap := data["config"].(map[string]any)
	secMap := cfgMap["security"].(map[string]any)
	masked := secMap["secret"].(string)

	if !strings.HasPrefix(masked, "mel...") && masked != "******" {
		t.Errorf("expected masked secret, got %s", masked)
	}

	// 2. With include_secrets
	req2, _ := http.NewRequest("GET", ts.URL+"/internal/config?include_secrets=true", nil)
	req2.Header.Set("X-Internal-Secret", secret)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("failed request: %v", err)
	}
	defer resp2.Body.Close()

	var data2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&data2)

	cfgMap2 := data2["config"].(map[string]any)
	secMap2 := cfgMap2["security"].(map[string]any)
	if secMap2["secret"] != secret {
		t.Errorf("expected full secret %s, got %s", secret, secMap2["secret"])
	}
}

func TestIPFiltering(t *testing.T) {
	if auth.IsAllowedInternalIP("8.8.8.8") != false {
		t.Error("8.8.8.8 should not be allowed")
	}
	if auth.IsAllowedInternalIP("93.184.216.34") != false {
		t.Error("93.184.216.34 should not be allowed")
	}
	if auth.IsAllowedInternalIP("172.18.0.2") != true {
		t.Error("172.18.0.2 should be allowed")
	}
	if auth.IsAllowedInternalIP("192.168.1.50") != true {
		t.Error("192.168.1.50 should be allowed")
	}
	if auth.IsAllowedInternalIP("10.0.0.1") != true {
		t.Error("10.0.0.1 should be allowed")
	}
	if auth.IsAllowedInternalIP("127.0.0.1") != true {
		t.Error("127.0.0.1 should be allowed")
	}
}

func TestWebSocketWorkflow(t *testing.T) {
	ts, _, _, _ := setupTestServer(t)
	defer ts.Close()

	u, _ := url.Parse(ts.URL)
	u.Scheme = "ws"
	u.Path = "/ws"

	// Connect WebSocket
	ws, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("failed to dial WS: %v", err)
	}
	defer ws.Close()
	defer resp.Body.Close()

	// 1. First message must be "connected"
	var connectedMsg map[string]any
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := ws.ReadJSON(&connectedMsg); err != nil {
		t.Fatalf("failed to read connected message: %v", err)
	}
	if connectedMsg["event"] != "connected" {
		t.Errorf("expected 'connected' event, got %v", connectedMsg["event"])
	}

	// 2. Send ping -> receive pong
	if err := ws.WriteJSON(map[string]string{"action": "ping"}); err != nil {
		t.Fatalf("failed to write ping: %v", err)
	}

	var pongMsg map[string]any
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := ws.ReadJSON(&pongMsg); err != nil {
		t.Fatalf("failed to read pong: %v", err)
	}
	if pongMsg["event"] != "pong" {
		t.Errorf("expected 'pong' event, got %v", pongMsg["event"])
	}

	// 3. Subscribe to channel
	if err := ws.WriteJSON(map[string]string{"action": "subscribe", "channel": "orders"}); err != nil {
		t.Fatalf("failed to send subscribe: %v", err)
	}

	var subMsg map[string]any
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := ws.ReadJSON(&subMsg); err != nil {
		t.Fatalf("failed to read subscribe response: %v", err)
	}
	if subMsg["event"] != "subscribed" {
		t.Errorf("expected 'subscribed' event, got %v", subMsg["event"])
	}

	// 4. Test Broadcast receipt
	broadcastPayload := map[string]any{
		"event":    "new_order",
		"data":     map[string]any{"id": 999},
		"channels": []string{"orders"},
	}
	bData, _ := json.Marshal(broadcastPayload)
	bReq, _ := http.NewRequest("POST", ts.URL+"/internal/broadcast", bytes.NewReader(bData))
	bReq.Header.Set("Content-Type", "application/json")
	bReq.Header.Set("X-Internal-Secret", "melodi_ws_secret_456")

	bResp, err := http.DefaultClient.Do(bReq)
	if err != nil {
		t.Fatalf("failed broadcast: %v", err)
	}
	_ = bResp.Body.Close()

	var bcMsg map[string]any
	_ = ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := ws.ReadJSON(&bcMsg); err != nil {
		t.Fatalf("failed to receive broadcast on WS: %v", err)
	}
	if bcMsg["event"] != "new_order" {
		t.Errorf("expected 'new_order' event, got %v", bcMsg["event"])
	}
}
