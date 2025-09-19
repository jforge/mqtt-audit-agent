// File: cmd/audit-agent/main.go
package main

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Config via env
type Config struct {
	BrokerURL          string   // e.g. tcp://mqtt:1883 or ssl://mqtt:8883
	ClientID           string   // persistent client id
	Username           string   // optional
	Password           string   // optional
	TopicFilters       []string // comma-separated list in env
	AuditTopicPrefix   string   // e.g. _audit/counts
	QoS                byte     // 0/1 (recommend 1)
	WindowSeconds      int      // tumbling window size in seconds (e.g., 5)
	LatenessSeconds    int      // allowed lateness / grace period (e.g., 20)
	EventTimeJSONField string   // name of JSON field carrying event-time, e.g. "timestamp"
	EventTimeFormat    string   // "rfc3339" or "unix_ms" or "unix_s"
	HealthAddr         string   // ":8080"
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func loadConfig() Config {
	topics := getenv("TOPIC_FILTERS", "#")
	qos := byte(1)
	if getenv("QOS", "1") == "0" {
		qos = 0
	}
	ws := 5
	if v := getenv("WINDOW_SECONDS", "5"); v != "" {
		fmt.Sscanf(v, "%d", &ws)
	}
	ls := 20
	if v := getenv("LATENESS_SECONDS", "20"); v != "" {
		fmt.Sscanf(v, "%d", &ls)
	}
	return Config{
		BrokerURL:          getenv("BROKER_URL", "tcp://localhost:1883"),
		ClientID:           getenv("CLIENT_ID", "audit-agent-1"),
		Username:           os.Getenv("MQTT_USERNAME"),
		Password:           os.Getenv("MQTT_PASSWORD"),
		TopicFilters:       splitAndTrim(topics),
		AuditTopicPrefix:   getenv("AUDIT_TOPIC_PREFIX", "_audit/counts"),
		QoS:                qos,
		WindowSeconds:      ws,
		LatenessSeconds:    ls,
		EventTimeJSONField: getenv("EVENT_TIME_JSON_FIELD", "timestamp"),
		EventTimeFormat:    getenv("EVENT_TIME_FORMAT", "rfc3339"),
		HealthAddr:         getenv("HEALTH_ADDR", ":8080"),
	}
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"#"}
	}
	return out
}

// Window key per topic-filter
type winKey struct{ Start time.Time }

type counter struct {
	Count        int64
	MaxEventTime time.Time
}

type store struct {
	mu sync.Mutex
	// map[filter]map[windowStart]counter
	m map[string]map[int64]*counter
}

func newStore() *store { return &store{m: make(map[string]map[int64]*counter)} }

func (s *store) incr(filter string, wStart time.Time, evtTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wm, ok := s.m[filter]
	if !ok {
		wm = make(map[int64]*counter)
		s.m[filter] = wm
	}
	k := wStart.Unix()
	c, ok := wm[k]
	if !ok {
		c = &counter{}
		wm[k] = c
	}
	c.Count++
	if evtTime.After(c.MaxEventTime) {
		c.MaxEventTime = evtTime
	}
}

func (s *store) closeDue(now time.Time, window time.Duration, grace time.Duration) (out []summary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for filter, wm := range s.m {
		for k, c := range wm {
			wStart := time.Unix(k, 0).UTC()
			wEnd := wStart.Add(window)
			if now.After(wEnd.Add(grace)) {
				out = append(out, summary{
					TopicPattern:  filter,
					WindowStart:   wStart,
					WindowEnd:     wEnd,
					Count:         c.Count,
					WatermarkSeen: now.UTC(),
				})
				delete(wm, k)
			}
		}
		if len(wm) == 0 {
			delete(s.m, filter)
		}
	}
	return
}

type summary struct {
	TopicPattern  string    `json:"topic_pattern"`
	WindowStart   time.Time `json:"window_start"`
	WindowEnd     time.Time `json:"window_end"`
	Count         int64     `json:"count"`
	AgentID       string    `json:"agent_id"`
	WatermarkSeen time.Time `json:"watermark_seen"`
	// Optional lightweight checksum to detect content duplication/corruption in-window
	PayloadCRC string `json:"payload_crc,omitempty"`
}

func main() {
	cfg := loadConfig()
	log.Printf("starting audit-agent; broker=%s topics=%v window=%ds grace=%ds qos=%d", cfg.BrokerURL, cfg.TopicFilters, cfg.WindowSeconds, cfg.LatenessSeconds, cfg.QoS)

	// health server
	go func() {
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
		if err := http.ListenAndServe(cfg.HealthAddr, nil); err != nil {
			log.Fatalf("health server error: %v", err)
		}
	}()

	// MQTT client setup
	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)
	opts.SetClientID(cfg.ClientID)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}
	opts.SetCleanSession(false) // persistent session
	opts.SetOrderMatters(false)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	st := newStore()
	window := time.Duration(cfg.WindowSeconds) * time.Second
	grace := time.Duration(cfg.LatenessSeconds) * time.Second

	// rolling CRC per (filter, window)
	crcMu := &sync.Mutex{}
	rollingCRC := make(map[string]map[int64]hash.Hash)

	// message handler
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := msg.Payload()
		evt, ok := extractEventTime(payload, cfg.EventTimeJSONField, cfg.EventTimeFormat)
		if !ok {
			// fallback to receive time
			evt = time.Now().UTC()
		}
		// compute window start aligned to epoch
		wStart := evt.Truncate(window)
		// increment all matching filters
		for _, f := range cfg.TopicFilters {
			if matchesFilter(f, topic) {
				st.incr(f, wStart, evt)
				// optional crc update
				crcMu.Lock()
				m, ok := rollingCRC[f]
				if !ok {
					m = make(map[int64]hash.Hash)
					rollingCRC[f] = m
				}
				key := wStart.Unix()
				h, ok := m[key]
				if !ok {
					h = sha1.New()
					m[key] = h
				}
				// minimal cost: incorporate a short digest of payload
				if len(payload) > 0 {
					b := payload
					if len(b) > 64 {
						b = b[:64]
					}
					h.Write(b)
				}
				crcMu.Unlock()
			}
		}
		msg.Ack() // no-op for paho v3, safe
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("mqtt connect failed: %v", token.Error())
	}
	log.Printf("connected to broker")

	// subscribe to filters
	for _, f := range cfg.TopicFilters {
		if t := client.Subscribe(f, cfg.QoS, nil); t.Wait() && t.Error() != nil {
			log.Fatalf("subscribe %s failed: %v", f, t.Error())
		}
		log.Printf("subscribed %q", f)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().UTC()
			due := st.closeDue(now, window, grace)
			for i := range due {
				s := &due[i]
				s.AgentID = cfg.ClientID
				// attach CRC if present
				crcMu.Lock()
				if m, ok := rollingCRC[s.TopicPattern]; ok {
					if h, ok2 := m[s.WindowStart.Unix()]; ok2 {
						sum := h.Sum(nil)
						s.PayloadCRC = base64.RawURLEncoding.EncodeToString(sum)
						delete(m, s.WindowStart.Unix())
						if len(m) == 0 {
							delete(rollingCRC, s.TopicPattern)
						}
					}
				}
				crcMu.Unlock()

				pubTopic := auditTopic(cfg.AuditTopicPrefix, s.TopicPattern)
				b, _ := json.Marshal(s)
				token := client.Publish(pubTopic, cfg.QoS, false, b)
				token.Wait()
				if err := token.Error(); err != nil {
					log.Printf("publish audit failed: %v", err)
				} else {
					log.Printf("audit %s w=[%s,%s) count=%d", s.TopicPattern, s.WindowStart.Format(time.RFC3339), s.WindowEnd.Format(time.RFC3339), s.Count)
				}
			}
		case <-ctx.Done():
			log.Printf("shutdown requested")
			client.Disconnect(250)
			return
		}
	}
}

func auditTopic(prefix, filter string) string {
	// URL-escape-like but keep MQTT-friendly
	// Replace '/' with '_', '+' with 'plus', '#' with 'hash'
	t := strings.ReplaceAll(filter, "/", "_")
	t = strings.ReplaceAll(t, "+", "plus")
	t = strings.ReplaceAll(t, "#", "hash")
	return fmt.Sprintf("%s/%s", strings.TrimSuffix(prefix, "/"), t)
}

func extractEventTime(payload []byte, field, format string) (time.Time, bool) {
	// Expect JSON payload and field present
	var m map[string]interface{}
	if err := json.Unmarshal(payload, &m); err != nil {
		return time.Time{}, false
	}
	v, ok := m[field]
	if !ok {
		return time.Time{}, false
	}
	switch strings.ToLower(format) {
	case "rfc3339":
		if s, ok := v.(string); ok {
			if ts, err := time.Parse(time.RFC3339, s); err == nil {
				return ts.UTC(), true
			}
		}
	case "unix_ms":
		switch x := v.(type) {
		case float64:
			return time.Unix(0, int64(x)*int64(time.Millisecond)).UTC(), true
		case json.Number:
			if n, err := x.Int64(); err == nil {
				return time.Unix(0, n*int64(time.Millisecond)).UTC(), true
			}
		}
	case "unix_s":
		switch x := v.(type) {
		case float64:
			return time.Unix(int64(x), 0).UTC(), true
		case json.Number:
			if n, err := x.Int64(); err == nil {
				return time.Unix(n, 0).UTC(), true
			}
		}
	}
	return time.Time{}, false
}

// MQTT wildcard matcher (single-level '+' and multi-level '#')
func matchesFilter(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := 0; i < len(f); i++ {
		if i >= len(t) {
			return false
		}
		switch f[i] {
		case "+":
			// matches exactly one level
		case "#":
			// matches remaining levels
			return true
		default:
			if f[i] != t[i] {
				return false
			}
		}
	}
	return len(f) == len(t)
}
