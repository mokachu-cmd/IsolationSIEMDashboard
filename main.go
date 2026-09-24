package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"isolation-siem-dashboard/pkg/ecs"
	"isolation-siem-dashboard/pkg/kafka"
	"isolation-siem-dashboard/pkg/storage"
	"isolation-siem-dashboard/pkg/websocket"
)

type RawAgentLog struct {
	AgentID      string  `json:"agent_id"`
	HostName     string  `json:"host_name"`
	EventType    string  `json:"event_type"`
	Message      string  `json:"message"`
	AnomalyScore float64 `json:"anomaly_score"` // Isolation Forest anomaly score (e.g., 0.0 to 1.0)
}

// Struct extending ECS payload with Severity & Anomaly details
type EvaluatedAlert struct {
	ecs.Event
	AnomalyScore float64 `json:"anomaly_score"`
	Severity     string  `json:"severity"`
}

var (
	kafkaProducer *kafka.Producer
	wsHub         *websocket.Hub
	esClient      *storage.ESClient
)

// Predetermined anomaly threshold (e.g., scores >= 0.75 trigger persistence)
const AnomalyThreshold = 0.75

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var rawLog RawAgentLog
	if err := json.NewDecoder(r.Body).Decode(&rawLog); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// 1. Normalize payload
	baseECS := ecs.NormalizeAgentPayload(rawLog.HostName, rawLog.EventType, rawLog.Message)

	// 2. Evaluate Anomaly Score against Threshold
	severity := "LOW"
	if rawLog.AnomalyScore >= AnomalyThreshold {
		severity = "HIGH_ANOMALY"
	}

	alertPayload := EvaluatedAlert{
		Event:        baseECS,
		AnomalyScore: rawLog.AnomalyScore,
		Severity:     severity,
	}

	payloadBytes, err := json.Marshal(alertPayload)
	if err != nil {
		http.Error(w, "Failed to serialize alert payload", http.StatusInternalServerError)
		return
	}

	// 3. Always stream live events to WebSocket
	wsHub.BroadcastEvent(payloadBytes)

	// 4. Threshold Check: Persist to Elasticsearch ONLY if score exceeds threshold
	ctx := context.Background()
	if rawLog.AnomalyScore >= AnomalyThreshold {
		docID := fmt.Sprintf("%s-%d", rawLog.HostName, time.Now().UnixNano())
		if esClient != nil {
			go func() {
				if err := esClient.IndexAlert(ctx, docID, payloadBytes); err != nil {
					log.Printf("[ELASTICSEARCH ERROR] %v", err)
				}
			}()
		}
		fmt.Printf("[THRESHOLD EXCEEDED] Score: %.2f >= %.2f | Persisting to Elasticsearch\n", rawLog.AnomalyScore, AnomalyThreshold)
	} else {
		fmt.Printf("[LOG INGESTED] Score: %.2f < %.2f | Skipped Elasticsearch persistence\n", rawLog.AnomalyScore, AnomalyThreshold)
	}

	// 5. Stream to Kafka (non-blocking)
	if kafkaProducer != nil {
		_ = kafkaProducer.PublishEvent(ctx, baseECS.Host.Name, payloadBytes)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ingested"}`))
}

func main() {
	wsHub = websocket.NewHub()
	go wsHub.Run()

	// Connect to Elasticsearch (Default local instance at http://127.0.0.1:9200)
	var err error
	esClient, err = storage.NewESClient("http://127.0.0.1:9200", "isolation-siem-alerts")
	if err != nil {
		log.Printf("[WARNING] Could not connect to Elasticsearch: %v. Running in streaming-only mode.", err)
	}

	kafkaProducer = kafka.NewProducer("127.0.0.1:9092", "telemetry.raw")
	defer kafkaProducer.Close()

	http.HandleFunc("/api/v1/ingest", ingestHandler)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHub.ServeWS(w, r)
	})

	port := "127.0.0.1:8080"
	fmt.Printf("IsolationSIEM Server listening on http://%s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}