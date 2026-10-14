package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sugvn/webhookv2/internal/model"
)

const (
	maxSnippet = 256      // bytes of response body kept for last_error
	maxDrain   = 64 << 10 // bytes read and discarded so the connection can be reused
)

type Sender struct{ client *http.Client }

func New() *Sender {
	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 16, // default is 2; many deliveries hit the same host
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	return &Sender{client: &http.Client{
		Transport: tr,
		// Never follow redirects: the 3xx is returned as-is and counted as a failure.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// Send makes one attempt. It never retries and never touches the DB.
func (s *Sender) Send(ctx context.Context, d model.Delivery) model.Outcome {
	body, err := json.Marshal(model.Payload{
		EventID:   d.EventUUID,
		EventType: d.EventType,
		Data:      d.DataBody,
	})
	if err != nil {
		return model.Outcome{Err: "marshal payload: " + err.Error()}
	}

	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	// One deadline for connect + write + headers + body read.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Subscriber, bytes.NewReader(body))
	if err != nil {
		return model.Outcome{Err: "build request: " + cleanErr(err)}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhookd/1")
	req.Header.Set("X-Delivery-Id", d.DeliveryID.String()) // stable across retries: dedup key
	req.Header.Set("X-Event-Id", d.EventUUID.String())
	req.Header.Set("X-Event-Type", d.EventType)
	req.Header.Set("X-Delivery-Attempt", strconv.Itoa(d.Attempts))

	resp, err := s.client.Do(req)
	if err != nil {
		return model.Outcome{Err: cleanErr(err)}
	}
	defer resp.Body.Close()

	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxSnippet))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))

	out := model.Outcome{StatusCode: resp.StatusCode}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		out.Err = fmt.Sprintf("status %d: %q", resp.StatusCode, snippet)
	}
	return out
}

func cleanErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err // drop the URL: it may carry userinfo credentials
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	return err.Error()
}
