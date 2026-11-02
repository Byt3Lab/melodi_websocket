# Multi-stage Dockerfile pour Melodi WebSocket (Go)

# Étape 1 : Compilation du binaire statique
FROM golang:1.23-alpine AS builder

WORKDIR /app
RUN apk add --no-cache git ca-certificates tzdata

# Cache des dépendances Go
COPY go.mod go.sum ./
RUN go mod download

# Copie du code source Go
COPY . .

# Compilation binaire statique CGO=0 sans symboles de debug (-s -w)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/melodi_websocket cmd/server/main.go

# Étape 2 : Image minimale de production Alpine
FROM alpine:latest

RUN apk add --no-cache ca-certificates curl tzdata
WORKDIR /app

# Copie du binaire compilé depuis l'étape de build
COPY --from=builder /app/melodi_websocket /app/melodi_websocket
RUN chmod +x /app/melodi_websocket

ENV PORT=8070 \
    SECRET=melodi_ws_secret_456 \
    WEBSOCKET_SCALE_MODE=autonome

EXPOSE 8070

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:${PORT:-8070}/internal/health || exit 1

CMD ["/app/melodi_websocket"]
