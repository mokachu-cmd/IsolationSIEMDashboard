package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ESClient struct {
	client *elasticsearch.Client
	index  string
}

// NewESClient initializes a connection to the Elasticsearch cluster
func NewESClient(address string, indexName string) (*ESClient, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{address},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	// Verify connectivity
	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("elasticsearch connection error: %w", err)
	}
	defer res.Body.Close()

	log.Printf("[ELASTICSEARCH] Connected successfully to %s", address)
	return &ESClient{
		client: es,
		index:  indexName,
	}, nil
}

// IndexAlert persists high-severity/anomaly events exceeding the threshold
func (es *ESClient) IndexAlert(ctx context.Context, docID string, payload []byte) error {
	req := esapi.IndexRequest{
		Index:      es.index,
		DocumentID: docID,
		Body:       bytes.NewReader(payload),
		Refresh:    "true",
	}

	res, err := req.Do(ctx, es.client)
	if err != nil {
		return fmt.Errorf("error indexing document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch error indexing doc [%s]: %s", docID, res.String())
	}

	log.Printf("[ELASTICSEARCH PERSISTED] Document ID: %s", docID)
	return nil
}