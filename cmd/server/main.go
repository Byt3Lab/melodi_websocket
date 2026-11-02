package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"

	"melodi_websocket/internal/api"
	"melodi_websocket/internal/config"
	"melodi_websocket/internal/hub"
	"melodi_websocket/internal/pubsub"
)

func main() {
	var (
		flagSecret         = flag.String("secret", "", "Internal shared secret key")
		flagPort           = flag.Int("port", 0, "WebSocket service port")
		flagMelodiPort     = flag.Int("melodi-port", 0, "Main Melodi application port")
		flagMelodiHost     = flag.String("melodi-host", "", "Main Melodi application host")
		flagMelodiPlatform = flag.String("melodi-platform", "", "Main Melodi application platform")
		flagEnv            = flag.String("env", "", "Environment")
		flagScaleMode      = flag.String("scale-mode", "", "Scale mode: autonome or scalable")
		flagRedisURL       = flag.String("redis-url", "", "Redis URL for scalable mode")
		flagRedisHost      = flag.String("redis-host", "", "Redis host")
		flagRedisPort      = flag.Int("redis-port", 0, "Redis port")
		flagRedisPassword  = flag.String("redis-password", "", "Redis password")
		flagRedisDB        = flag.Int("redis-db", 0, "Redis database index")
		flagRedisChannel   = flag.String("redis-channel", "", "Redis pub/sub channel name")
	)

	flag.Parse()

	flagsPassed := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		flagsPassed[f.Name] = true
	})

	cliOverrides := config.FlatConfig{
		Port:           *flagPort,
		Secret:         *flagSecret,
		MelodiPort:     *flagMelodiPort,
		MelodiHost:     *flagMelodiHost,
		MelodiPlatform: *flagMelodiPlatform,
		Env:            *flagEnv,
		ScaleMode:      *flagScaleMode,
		RedisURL:       *flagRedisURL,
		RedisHost:      *flagRedisHost,
		RedisPort:      *flagRedisPort,
		RedisPassword:  *flagRedisPassword,
		RedisDB:        *flagRedisDB,
		RedisChannel:   *flagRedisChannel,
	}

	nodeID := fmt.Sprintf("node_%s", uuid.New().String()[:8])
	cfgMgr := config.NewConfigManager("storage", "config.json")
	activeCfg := cfgMgr.InitConfig(cliOverrides, flagsPassed)

	log.Printf("[Melodi WebSocket] Démarrage en mode '%s' (node_id=%s)\n", activeCfg.ScaleMode, nodeID)

	var psBackend pubsub.Backend
	if activeCfg.ScaleMode == "scalable" || activeCfg.ScaleMode == "cluster" {
		log.Println("[Melodi WebSocket] Vérification obligatoire de la connexion Redis...")
		rBackend := pubsub.NewRedisPubSub(
			activeCfg.RedisURL,
			activeCfg.RedisHost,
			activeCfg.RedisPort,
			activeCfg.RedisPassword,
			activeCfg.RedisDB,
			activeCfg.RedisChannel,
			nodeID,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ok, err := rBackend.CheckConnection(ctx)
		cancel()

		if !ok {
			log.Fatalf("[Melodi WebSocket] ÉCHEC CRITIQUE : Impossible de joindre Redis en mode scalable (%s). Le serveur ne démarrera pas.\n", err)
		}
		log.Println("[Melodi WebSocket] Connexion Redis validée avec succès.")
		psBackend = rBackend
	} else {
		psBackend = pubsub.NewLocalPubSub()
	}

	hubMgr := hub.NewConnectionManager(activeCfg.GracePeriodSeconds, nodeID)
	hubMgr.SetPubsubPublisher(func(ctx context.Context, env pubsub.Envelope) (int, error) {
		return psBackend.Publish(ctx, env)
	})

	// Start pubsub message listener
	err := psBackend.Start(context.Background(), func(env pubsub.Envelope) int {
		return hubMgr.LocalBroadcast(env.Message, env.UserIDs, env.Channels)
	})
	if err != nil {
		log.Fatalf("[Melodi WebSocket] Échec d'activation du PubSub : %v\n", err)
	}

	server := api.NewServer(cfgMgr, hubMgr, psBackend, nodeID)

	addr := net.JoinHostPort(activeCfg.Host, strconv.Itoa(activeCfg.Port))
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      server.Router(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("Starting Melodi WebSocket Service on http://%s (node: %s)\n", addr, nodeID)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v\n", err)
		}
	}()

	// Wait for shutdown signal or internal restart request
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-stopSig:
		log.Printf("[Melodi WebSocket] Arrêt reçu via signal (%v)...\n", sig)
	case <-server.ShutdownChannel():
		log.Println("[Melodi WebSocket] Arrêt programmé demandé par Melodi ERP.")
	}

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(shutdownCtx)
	_ = psBackend.Stop()
	log.Println("[Melodi WebSocket] Service arrêté proprement.")
}
