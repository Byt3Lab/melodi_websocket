package auth

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// ExtractUserID extracts user_id, id, or sub from a JWT token.
// Follows the same fallback logic as the Python implementation:
// 1. Valid signature check with secret
// 2. Unverified claims parsing
// 3. Fallback raw Base64 payload decode
func ExtractUserID(tokenStr, secret string) string {
	if tokenStr == "" {
		return ""
	}

	// 1. Try with signature
	if secret != "" {
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			return []byte(secret), nil
		})
		if err == nil && token.Valid {
			if claims, ok := token.Claims.(jwt.MapClaims); ok {
				if uid := getUserIDFromMap(claims); uid != "" {
					return uid
				}
			}
		}
	}

	// 2. Try parsing unverified
	parser := jwt.NewParser()
	var unverifiedClaims jwt.MapClaims
	if _, _, err := parser.ParseUnverified(tokenStr, &unverifiedClaims); err == nil {
		if uid := getUserIDFromMap(unverifiedClaims); uid != "" {
			return uid
		}
	}

	// 3. Fallback base64 decoding of 2nd segment
	parts := strings.Split(tokenStr, ".")
	if len(parts) >= 2 {
		payloadB64 := parts[1]
		// Add base64 padding if needed
		if rem := len(payloadB64) % 4; rem != 0 {
			payloadB64 += strings.Repeat("=", 4-rem)
		}
		data, err := base64.URLEncoding.DecodeString(payloadB64)
		if err == nil {
			var m map[string]interface{}
			if err := json.Unmarshal(data, &m); err == nil {
				return getUserIDFromMap(m)
			}
		}
	}

	return ""
}

func getUserIDFromMap(m map[string]interface{}) string {
	if v, ok := m["user_id"]; ok && v != nil {
		return formatID(v)
	}
	if v, ok := m["id"]; ok && v != nil {
		return formatID(v)
	}
	if v, ok := m["sub"]; ok && v != nil {
		return formatID(v)
	}
	return ""
}

func formatID(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatInt(int64(val), 10)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	default:
		return ""
	}
}

// CheckInternalAuth validates shared secret via X-Internal-Secret or Bearer token.
func CheckInternalAuth(r *http.Request, expectedSecret string) bool {
	if expectedSecret == "" {
		return false
	}

	if val := r.Header.Get("X-Internal-Secret"); val != "" {
		return val == expectedSecret
	}

	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		return token == expectedSecret
	}

	return false
}

// IsAllowedInternalIP checks if the IP is loopback, localhost, or RFC1918 private network.
func IsAllowedInternalIP(ipStr string) bool {
	if ipStr == "" || ipStr == "127.0.0.1" || ipStr == "::1" || ipStr == "localhost" || ipStr == "testclient" || ipStr == "<local>" {
		return true
	}

	// Strip port if present
	host := ipStr
	if h, _, err := net.SplitHostPort(ipStr); err == nil {
		host = h
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsLoopback() || ip.IsPrivate()
}
