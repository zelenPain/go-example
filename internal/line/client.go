package line

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

type Message struct {
	To   string `json:"to"`
	Text string `json:"text"`
}

func NewClient(endpoint, token string) Client {
	return Client{
		endpoint: endpoint,
		token:    token,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c Client) Send(ctx context.Context, msg Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("line endpoint returned status %d", resp.StatusCode)
	}
	return nil
}
