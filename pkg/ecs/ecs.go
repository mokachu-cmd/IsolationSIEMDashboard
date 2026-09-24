package ecs

import (
	"time"
)

// Base ECS Event Structure
type Event struct {
	Timestamp time.Time `json:"@timestamp"`
	Host      Host      `json:"host"`
	Event     EventMeta `json:"event"`
	Process   *Process  `json:"process,omitempty"`
	Log       LogMeta   `json:"log"`
}

type Host struct {
	Name string `json:"name"`
	ID   string `json:"id,omitempty"`
}

type EventMeta struct {
	Kind     string `json:"kind"`     // e.g., "event", "alert"
	Category string `json:"category"` // e.g., "process", "network", "authentication"
	Type     string `json:"type"`     // e.g., "start", "access", "info"
	Action   string `json:"action"`   // e.g., "process-created"
}

type Process struct {
	Name string `json:"name,omitempty"`
	PID  int64  `json:"pid,omitempty"`
	Path string `json:"path,omitempty"`
}

type LogMeta struct {
	Original string `json:"original"` // Raw incoming log text
}

// NormalizeAgentPayload converts raw incoming JSON telemetry into standard ECS format
func NormalizeAgentPayload(rawHost string, rawEventType string, rawMessage string) Event {
	return Event{
		Timestamp: time.Now().UTC(),
		Host: Host{
			Name: rawHost,
		},
		Event: EventMeta{
			Kind:     "event",
			Category: "host",
			Type:     rawEventType,
			Action:   rawEventType,
		},
		Log: LogMeta{
			Original: rawMessage,
		},
	}
}