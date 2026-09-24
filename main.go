package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"isolation-siem-dashboard/pkg/ecs"
	"isolation-siem-dashboard/pkg/kafka"
	"isolation-siem-dashboard/pkg/websocket"
)

type RawAgentLog struct {
	AgentID   string `json:"agent_id"`
	HostName  string `json:"host_name"`
	EventType string `json:"event_type"`
	Message   string `json:"message"`
}

var (
	kafkaProducer *kafka.Producer
	wsHub         *websocket.Hub
)

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

	// 1. Normalize raw log payload to ECS standard
	ecsEvent := ecs.NormalizeAgentPayload(rawLog.HostName, rawLog.EventType, rawLog.Message)

	ecsBytes, err := json.Marshal(ecsEvent)
	if err != nil {
		http.Error(w, "Failed to serialize ECS event", http.StatusInternalServerError)
		return
	}

	// 2. Broadcast live event directly to connected WebSockets (React UI)
	wsHub.BroadcastEvent(ecsBytes)

	// 3. Publish normalized ECS payload to Kafka (non-blocking)
	ctx := context.Background()
	if kafkaProducer != nil {
		_ = kafkaProducer.PublishEvent(ctx, ecsEvent.Host.Name, ecsBytes)
	}

	fmt.Printf("[INGESTED & BROADCAST] Host: %s | Action: %s\n", ecsEvent.Host.Name, ecsEvent.Event.Action)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ingested_and_streamed"}`))
}

func main() {
	// Initialize WebSocket Hub
	wsHub = websocket.NewHub()
	go wsHub.Run()

	// Initialize Kafka Producer (Optional if testing without Docker)
	kafkaProducer = kafka.NewProducer("127.0.0.1:9092", "telemetry.raw")
	defer kafkaProducer.Close()

	// HTTP Routing
	http.HandleFunc("/api/v1/ingest", ingestHandler)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHub.ServeWS(w, r)
	})

	port := "127.0.0.1:8080"
	fmt.Printf("IsolationSIEM Server listening on http://%s...\n", port)
	fmt.Printf("WebSocket endpoint live at ws://%s/ws\n", port)

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}