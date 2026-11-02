package config

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ServerConfig holds the network and server parameters.
type ServerConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Env      string `json:"env"`
	LogLevel string `json:"log_level"`
}

// SecurityConfig holds secret and IP filtering configuration.
type SecurityConfig struct {
	Secret               string `json:"secret"`
	SecretConfigured     bool   `json:"secret_configured"`
	AllowPrivateInternal bool   `json:"allow_private_internal"`
}

// IntegrationConfig holds Melodi ERP backend connection details.
type IntegrationConfig struct {
	MelodiHost     string `json:"melodi_host"`
	MelodiPort     int    `json:"melodi_port"`
	MelodiPlatform string `json:"melodi_platform"`
}

// PubSubConfig holds pub/sub clustering settings.
type PubSubConfig struct {
	ScaleMode     string `json:"scale_mode"`
	RedisURL      string `json:"redis_url,omitempty"`
	RedisHost     string `json:"redis_host"`
	RedisPort     int    `json:"redis_port"`
	RedisPassword string `json:"redis_password,omitempty"`
	RedisDB       int    `json:"redis_db"`
	RedisChannel  string `json:"redis_channel"`
}

// RuntimeConfig holds connection and session tuning parameters.
type RuntimeConfig struct {
	GracePeriodSeconds  int `json:"grace_period_seconds"`
	MaxConnections      int `json:"max_connections"`
	PingIntervalSeconds int `json:"ping_interval_seconds"`
}

// StorageConfig holds persistence metadata.
type StorageConfig struct {
	ConfigFile     string `json:"config_file"`
	FilePersistent bool   `json:"file_persistent"`
	LastModified   string `json:"last_modified,omitempty"`
}

// StructuredConfig represents the structured config response for Melodi ERP.
type StructuredConfig struct {
	Server      ServerConfig      `json:"server"`
	Security    SecurityConfig    `json:"security"`
	Integration IntegrationConfig `json:"integration"`
	PubSub      PubSubConfig      `json:"pubsub"`
	Runtime     RuntimeConfig     `json:"runtime"`
	Storage     StorageConfig     `json:"storage"`
}

// FlatConfig is the internal flat representation used for storage/config.json.
type FlatConfig struct {
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	Env                  string `json:"env"`
	LogLevel             string `json:"log_level"`
	Secret               string `json:"secret"`
	AllowPrivateInternal bool   `json:"allow_private_internal"`
	MelodiHost           string `json:"melodi_host"`
	MelodiPort           int    `json:"melodi_port"`
	MelodiPlatform       string `json:"melodi_platform"`
	ScaleMode            string `json:"scale_mode"`
	RedisURL             string `json:"redis_url,omitempty"`
	RedisHost            string `json:"redis_host"`
	RedisPort            int    `json:"redis_port"`
	RedisPassword        string `json:"redis_password,omitempty"`
	RedisDB              int    `json:"redis_db"`
	RedisChannel         string `json:"redis_channel"`
	GracePeriodSeconds   int    `json:"grace_period_seconds"`
	MaxConnections       int    `json:"max_connections"`
	PingIntervalSeconds  int    `json:"ping_interval_seconds"`
}

// DefaultConfig provides factory defaults.
func DefaultConfig() FlatConfig {
	return FlatConfig{
		Host:                 "0.0.0.0",
		Port:                 8002,
		Env:                  "dev",
		LogLevel:             "INFO",
		Secret:               "melodi_ws_secret_456",
		AllowPrivateInternal: true,
		MelodiHost:           "localhost",
		MelodiPort:           3000,
		MelodiPlatform:       "linux",
		ScaleMode:            "autonome",
		RedisURL:             "",
		RedisHost:            "localhost",
		RedisPort:            6379,
		RedisPassword:        "",
		RedisDB:              0,
		RedisChannel:         "melodi:ws:events",
		GracePeriodSeconds:   180,
		MaxConnections:       10000,
		PingIntervalSeconds:  30,
	}
}

// MaskSecret masks sensitive credentials for safe logging and administration.
func MaskSecret(val string) string {
	if val == "" {
		return ""
	}
	if len(val) <= 6 {
		return "******"
	}
	return fmt.Sprintf("%s...%s", val[:3], val[len(val)-3:])
}

// ConfigManager handles dynamic configuration, hot reloading, and JSON file persistence.
type ConfigManager struct {
	mu            sync.RWMutex
	current       FlatConfig
	startupConfig FlatConfig
	configDir     string
	configFile    string
	StartTime     time.Time
}

// NewConfigManager initializes a new ConfigManager.
func NewConfigManager(configDir, configFilename string) *ConfigManager {
	if configDir == "" {
		configDir = "storage"
	}
	if configFilename == "" {
		configFilename = "config.json"
	}

	return &ConfigManager{
		current:    DefaultConfig(),
		configDir:  configDir,
		configFile: filepath.Join(configDir, configFilename),
		StartTime:  time.Now(),
	}
}

// InitConfig applies layers in order: defaults -> disk file -> env vars -> CLI flags.
func (cm *ConfigManager) InitConfig(cliOverrides FlatConfig, cliFlagsPassed map[string]bool) FlatConfig {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cfg := DefaultConfig()

	// 1. Load from disk if present
	if diskData, err := cm.loadFromDisk(); err == nil && diskData != nil {
		cm.mergeFlat(&cfg, *diskData)
	}

	// 2. Load from environment variables
	cm.loadFromEnv(&cfg)

	// 3. Apply CLI overrides
	cm.mergeCliFlags(&cfg, cliOverrides, cliFlagsPassed)

	cm.current = cfg
	cm.startupConfig = cfg
	return cfg
}

func (cm *ConfigManager) loadFromDisk() (*FlatConfig, error) {
	if _, err := os.Stat(cm.configFile); os.IsNotExist(err) {
		return nil, nil
	}
	data, err := os.ReadFile(cm.configFile)
	if err != nil {
		return nil, err
	}
	var fileCfg FlatConfig
	if err := json.Unmarshal(data, &fileCfg); err != nil {
		return nil, err
	}
	return &fileCfg, nil
}

func (cm *ConfigManager) loadFromEnv(cfg *FlatConfig) {
	if v := os.Getenv("MELODI_WS_HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("MELODI_WS_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Port = p
		}
	}
	if v := os.Getenv("MELODI_WS_SECRET"); v != "" {
		cfg.Secret = v
	}
	if v := os.Getenv("MELODI_WS_SCALE_MODE"); v != "" {
		cfg.ScaleMode = v
	}
	if v := os.Getenv("MELODI_WS_REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("MELODI_WS_REDIS_HOST"); v != "" {
		cfg.RedisHost = v
	}
	if v := os.Getenv("MELODI_WS_REDIS_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.RedisPort = p
		}
	}
	if v := os.Getenv("MELODI_WS_REDIS_PASSWORD"); v != "" {
		cfg.RedisPassword = v
	}
	if v := os.Getenv("MELODI_WS_REDIS_CHANNEL"); v != "" {
		cfg.RedisChannel = v
	}
	if v := os.Getenv("MELODI_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.MelodiPort = p
		}
	}
	if v := os.Getenv("MELODI_HOST"); v != "" {
		cfg.MelodiHost = v
	}
	if v := os.Getenv("MELODI_PLATFORM"); v != "" {
		cfg.MelodiPlatform = v
	}
}

func (cm *ConfigManager) mergeFlat(dst *FlatConfig, src FlatConfig) {
	if src.Host != "" {
		dst.Host = src.Host
	}
	if src.Port > 0 {
		dst.Port = src.Port
	}
	if src.Env != "" {
		dst.Env = src.Env
	}
	if src.LogLevel != "" {
		dst.LogLevel = src.LogLevel
	}
	if src.Secret != "" {
		dst.Secret = src.Secret
	}
	dst.AllowPrivateInternal = src.AllowPrivateInternal
	if src.MelodiHost != "" {
		dst.MelodiHost = src.MelodiHost
	}
	if src.MelodiPort > 0 {
		dst.MelodiPort = src.MelodiPort
	}
	if src.MelodiPlatform != "" {
		dst.MelodiPlatform = src.MelodiPlatform
	}
	if src.ScaleMode != "" {
		dst.ScaleMode = src.ScaleMode
	}
	if src.RedisURL != "" {
		dst.RedisURL = src.RedisURL
	}
	if src.RedisHost != "" {
		dst.RedisHost = src.RedisHost
	}
	if src.RedisPort > 0 {
		dst.RedisPort = src.RedisPort
	}
	if src.RedisPassword != "" {
		dst.RedisPassword = src.RedisPassword
	}
	if src.RedisChannel != "" {
		dst.RedisChannel = src.RedisChannel
	}
	if src.GracePeriodSeconds > 0 {
		dst.GracePeriodSeconds = src.GracePeriodSeconds
	}
	if src.MaxConnections > 0 {
		dst.MaxConnections = src.MaxConnections
	}
	if src.PingIntervalSeconds > 0 {
		dst.PingIntervalSeconds = src.PingIntervalSeconds
	}
}

func (cm *ConfigManager) mergeCliFlags(dst *FlatConfig, cli FlatConfig, flagsPassed map[string]bool) {
	if flagsPassed["port"] && cli.Port > 0 {
		dst.Port = cli.Port
	}
	if flagsPassed["secret"] && cli.Secret != "" {
		dst.Secret = cli.Secret
	}
	if flagsPassed["melodi-port"] && cli.MelodiPort > 0 {
		dst.MelodiPort = cli.MelodiPort
	}
	if flagsPassed["melodi-host"] && cli.MelodiHost != "" {
		dst.MelodiHost = cli.MelodiHost
	}
	if flagsPassed["melodi-platform"] && cli.MelodiPlatform != "" {
		dst.MelodiPlatform = cli.MelodiPlatform
	}
	if flagsPassed["env"] && cli.Env != "" {
		dst.Env = cli.Env
	}
	if flagsPassed["scale-mode"] && cli.ScaleMode != "" {
		dst.ScaleMode = cli.ScaleMode
	}
	if flagsPassed["redis-url"] && cli.RedisURL != "" {
		dst.RedisURL = cli.RedisURL
	}
	if flagsPassed["redis-host"] && cli.RedisHost != "" {
		dst.RedisHost = cli.RedisHost
	}
	if flagsPassed["redis-port"] && cli.RedisPort > 0 {
		dst.RedisPort = cli.RedisPort
	}
	if flagsPassed["redis-password"] && cli.RedisPassword != "" {
		dst.RedisPassword = cli.RedisPassword
	}
	if flagsPassed["redis-channel"] && cli.RedisChannel != "" {
		dst.RedisChannel = cli.RedisChannel
	}
}

// Get returns the current configuration copy.
func (cm *ConfigManager) Get() FlatConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.current
}

// ConfigFilePath returns the absolute path to the config file.
func (cm *ConfigManager) ConfigFilePath() string {
	abs, err := filepath.Abs(cm.configFile)
	if err != nil {
		return cm.configFile
	}
	return abs
}

// SaveToDisk writes the current configuration atomically to disk.
func (cm *ConfigManager) SaveToDisk() error {
	cm.mu.RLock()
	data, err := json.MarshalIndent(cm.current, "", "  ")
	cm.mu.RUnlock()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cm.configDir, 0755); err != nil {
		return err
	}

	tempFile := fmt.Sprintf("%s.tmp.%d", cm.configFile, os.Getpid())
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempFile, cm.configFile)
}

// ResetToStartup resets the configuration to startup values.
func (cm *ConfigManager) ResetToStartup() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.current = cm.startupConfig
	_ = os.Remove(cm.configFile)
}

// UpdateConfig updates fields dynamically and optionally persists to disk.
func (cm *ConfigManager) UpdateConfig(updates map[string]any, persist bool) (bool, []string, bool, string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var updatedKeys []string
	restartRequired := false

	for k, v := range updates {
		if k == "persist" {
			continue
		}
		switch k {
		case "grace_period_seconds":
			if val, ok := toInt(v); ok && val >= 0 {
				cm.current.GracePeriodSeconds = val
				updatedKeys = append(updatedKeys, k)
			}
		case "log_level":
			if s, ok := v.(string); ok && s != "" {
				cm.current.LogLevel = strings.ToUpper(s)
				updatedKeys = append(updatedKeys, k)
			}
		case "secret":
			if s, ok := v.(string); ok && s != "" {
				cm.current.Secret = s
				updatedKeys = append(updatedKeys, k)
			}
		case "scale_mode":
			if s, ok := v.(string); ok && (s == "autonome" || s == "scalable" || s == "cluster") {
				cm.current.ScaleMode = s
				updatedKeys = append(updatedKeys, k)
			}
		case "redis_url":
			if s, ok := v.(string); ok {
				cm.current.RedisURL = s
				updatedKeys = append(updatedKeys, k)
			}
		case "redis_host":
			if s, ok := v.(string); ok && s != "" {
				cm.current.RedisHost = s
				updatedKeys = append(updatedKeys, k)
			}
		case "redis_port":
			if val, ok := toInt(v); ok && val > 0 {
				cm.current.RedisPort = val
				updatedKeys = append(updatedKeys, k)
			}
		case "redis_password":
			if s, ok := v.(string); ok {
				cm.current.RedisPassword = s
				updatedKeys = append(updatedKeys, k)
			}
		case "redis_db":
			if val, ok := toInt(v); ok && val >= 0 {
				cm.current.RedisDB = val
				updatedKeys = append(updatedKeys, k)
			}
		case "redis_channel":
			if s, ok := v.(string); ok && s != "" {
				cm.current.RedisChannel = s
				updatedKeys = append(updatedKeys, k)
			}
		case "port":
			if val, ok := toInt(v); ok && val > 0 && val != cm.current.Port {
				cm.current.Port = val
				updatedKeys = append(updatedKeys, k)
				restartRequired = true
			}
		case "host":
			if s, ok := v.(string); ok && s != "" && s != cm.current.Host {
				cm.current.Host = s
				updatedKeys = append(updatedKeys, k)
				restartRequired = true
			}
		case "allow_private_internal":
			if b, ok := v.(bool); ok {
				cm.current.AllowPrivateInternal = b
				updatedKeys = append(updatedKeys, k)
			}
		case "melodi_host":
			if s, ok := v.(string); ok && s != "" {
				cm.current.MelodiHost = s
				updatedKeys = append(updatedKeys, k)
			}
		case "melodi_port":
			if val, ok := toInt(v); ok && val > 0 {
				cm.current.MelodiPort = val
				updatedKeys = append(updatedKeys, k)
			}
		}
	}

	if persist {
		data, err := json.MarshalIndent(cm.current, "", "  ")
		if err == nil {
			_ = os.MkdirAll(cm.configDir, 0755)
			tempFile := fmt.Sprintf("%s.tmp.%d", cm.configFile, os.Getpid())
			if err := os.WriteFile(tempFile, data, 0644); err == nil {
				_ = os.Rename(tempFile, cm.configFile)
			}
		}
	}

	return true, updatedKeys, restartRequired, "Configuration mise à jour avec succès"
}

func toInt(v any) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case float64:
		return int(val), true
	case string:
		i, err := strconv.Atoi(val)
		return i, err == nil
	default:
		return 0, false
	}
}

// GetStructured produces the formatted representation for Melodi ERP admin.
func (cm *ConfigManager) GetStructured(includeSecrets bool) StructuredConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	c := cm.current

	secValue := c.Secret
	redisPassValue := c.RedisPassword
	if !includeSecrets {
		secValue = MaskSecret(secValue)
		redisPassValue = MaskSecret(redisPassValue)
	}

	filePersistent := false
	var lastModified string
	if fi, err := os.Stat(cm.configFile); err == nil {
		filePersistent = true
		lastModified = fi.ModTime().UTC().Format(time.RFC3339)
	}

	absPath, _ := filepath.Abs(cm.configFile)

	return StructuredConfig{
		Server: ServerConfig{
			Host:     c.Host,
			Port:     c.Port,
			Env:      c.Env,
			LogLevel: c.LogLevel,
		},
		Security: SecurityConfig{
			Secret:               secValue,
			SecretConfigured:     c.Secret != "",
			AllowPrivateInternal: c.AllowPrivateInternal,
		},
		Integration: IntegrationConfig{
			MelodiHost:     c.MelodiHost,
			MelodiPort:     c.MelodiPort,
			MelodiPlatform: c.MelodiPlatform,
		},
		PubSub: PubSubConfig{
			ScaleMode:     c.ScaleMode,
			RedisURL:      c.RedisURL,
			RedisHost:     c.RedisHost,
			RedisPort:     c.RedisPort,
			RedisPassword: redisPassValue,
			RedisDB:       c.RedisDB,
			RedisChannel:  c.RedisChannel,
		},
		Runtime: RuntimeConfig{
			GracePeriodSeconds:  c.GracePeriodSeconds,
			MaxConnections:      c.MaxConnections,
			PingIntervalSeconds: c.PingIntervalSeconds,
		},
		Storage: StorageConfig{
			ConfigFile:     absPath,
			FilePersistent: filePersistent,
			LastModified:   lastModified,
		},
	}
}

// TestRedisConnection verifies Redis connectivity and latency.
func TestRedisConnection(ctx context.Context, host string, port int, password, redisURL string, db int) (bool, float64, string) {
	var opt *redis.Options
	var err error

	if redisURL != "" {
		opt, err = redis.ParseURL(redisURL)
		if err != nil {
			return false, 0, fmt.Sprintf("URL Redis invalide: %v", err)
		}
	} else {
		opt = &redis.Options{
			Addr:     net.JoinHostPort(host, strconv.Itoa(port)),
			Password: password,
			DB:       db,
		}
	}

	client := redis.NewClient(opt)
	defer client.Close()

	start := time.Now()
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	cmd := client.Ping(pingCtx)
	if err := cmd.Err(); err != nil {
		return false, 0, fmt.Sprintf("Échec ping Redis: %v", err)
	}

	latency := float64(time.Since(start).Microseconds()) / 1000.0
	return true, latency, "Connexion Redis opérationnelle"
}

// TestMelodiConnection checks TCP or HTTP reachability to Melodi ERP port.
func TestMelodiConnection(ctx context.Context, host string, port int) (bool, float64, string) {
	target := net.JoinHostPort(host, strconv.Itoa(port))
	start := time.Now()

	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		return false, 0, fmt.Sprintf("Impossible de joindre Melodi ERP sur %s: %v", target, err)
	}
	_ = conn.Close()

	latency := float64(time.Since(start).Microseconds()) / 1000.0

	// Try quick HTTP ping
	url := fmt.Sprintf("http://%s/api/health", target)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	client := http.Client{Timeout: 2 * time.Second}
	if resp, err := client.Do(req); err == nil {
		_ = resp.Body.Close()
	}

	return true, latency, fmt.Sprintf("Port Melodi ERP %d accessible (%0.2fms)", port, latency)
}
