package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCalculateDurationStats(t *testing.T) {
	values := []time.Duration{
		100 * time.Millisecond,
		10 * time.Millisecond,
		50 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
	}
	got := calculateDurationStats(values)
	if got.Count != 5 || got.P50MS != 30 || got.P95MS != 100 || got.P99MS != 100 || got.MaxMS != 100 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestLoadHTTPClientSizesConnectionPoolToConcurrency(t *testing.T) {
	client := newLoadHTTPClient(3*time.Second, 250)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T", client.Transport)
	}
	if client.Timeout != 3*time.Second || transport.MaxIdleConns != 250 || transport.MaxIdleConnsPerHost != 250 || transport.MaxConnsPerHost != 250 {
		t.Fatalf("client timeout=%s transport=%+v", client.Timeout, transport)
	}
	transport.CloseIdleConnections()
}

func TestTrackerDeduplicatesByConnection(t *testing.T) {
	tracked := newTracker()
	tracked.expect("message-00000001", time.Now().Add(-time.Millisecond), []string{"phone", "desktop"})
	event := wsEnvelope{Message: apiMessage{ClientMessageID: "message-00000001"}}
	tracked.observe("phone", event)
	tracked.observe("phone", event)
	tracked.observe("desktop", event)

	expected, observed, missing, latencies, duplicates, unexpected, _ := tracked.snapshot()
	if expected != 2 || observed != 2 || len(missing) != 0 || len(latencies) != 2 || duplicates != 1 || unexpected != 0 {
		t.Fatalf("snapshot = expected %d observed %d missing %v latencies %d duplicates %d unexpected %d",
			expected, observed, missing, len(latencies), duplicates, unexpected)
	}
}

func TestTrackerObservesManyMessagesConcurrently(t *testing.T) {
	const messages = 2048
	tracked := newTracker()
	for i := 0; i < messages; i++ {
		clientID := fmt.Sprintf("message-%08d", i)
		tracked.expect(clientID, time.Now(), []string{"phone", "desktop"})
	}

	var group sync.WaitGroup
	for i := 0; i < messages; i++ {
		clientID := fmt.Sprintf("message-%08d", i)
		for _, connectionID := range []string{"phone", "desktop"} {
			group.Add(1)
			go func(connectionID, clientID string) {
				defer group.Done()
				tracked.observe(connectionID, wsEnvelope{Message: apiMessage{ClientMessageID: clientID}})
			}(connectionID, clientID)
		}
	}
	group.Wait()

	expected, observed, missing, latencies, duplicates, unexpected, readerErrors := tracked.snapshot()
	if expected != messages*2 || observed != messages*2 || len(missing) != 0 || len(latencies) != messages*2 || duplicates != 0 || unexpected != 0 || len(readerErrors) != 0 {
		t.Fatalf("snapshot = expected %d observed %d missing %d latencies %d duplicates %d unexpected %d reader errors %d",
			expected, observed, len(missing), len(latencies), duplicates, unexpected, len(readerErrors))
	}
}

func TestMetricSelection(t *testing.T) {
	before := map[string]float64{"im_backend_http_requests_total": 10, "ignored": 2}
	after := map[string]float64{
		"im_backend_http_requests_total":   15,
		"im_backend_outbox_pending_events": 3,
		"ignored":                          4,
	}
	if got := metricDelta(before, after); !reflect.DeepEqual(got, map[string]float64{"im_backend_http_requests_total": 5}) {
		t.Fatalf("metric delta = %#v", got)
	}
	if got := selectedMetricEnd(after); !reflect.DeepEqual(got, map[string]float64{"im_backend_outbox_pending_events": 3}) {
		t.Fatalf("metric end = %#v", got)
	}
}

func TestFetchMetricsKeepsOutboxStageHistograms(t *testing.T) {
	exposition := `# HELP im_backend_outbox_pending_events pending
# TYPE im_backend_outbox_pending_events gauge
im_backend_outbox_pending_events 7
# HELP im_backend_outbox_stage_duration_seconds stages
# TYPE im_backend_outbox_stage_duration_seconds histogram
im_backend_outbox_stage_duration_seconds_bucket{stage="claim",le="0.1"} 1
im_backend_outbox_stage_duration_seconds_bucket{stage="claim",le="0.5"} 2
im_backend_outbox_stage_duration_seconds_bucket{stage="claim",le="+Inf"} 2
im_backend_outbox_stage_duration_seconds_sum{stage="claim"} 0.3
im_backend_outbox_stage_duration_seconds_count{stage="claim"} 2
`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exposition))
	}))
	defer server.Close()

	snapshot, err := fetchMetrics(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Values["im_backend_outbox_pending_events"] != 7 {
		t.Fatalf("pending metric = %v", snapshot.Values["im_backend_outbox_pending_events"])
	}
	claim := snapshot.OutboxStageDurations["claim"]
	if claim.Count != 2 || claim.Sum != 0.3 || claim.Buckets[0.1] != 1 || claim.Buckets[0.5] != 2 {
		t.Fatalf("claim histogram = %#v", claim)
	}
}

func TestOutboxStageDurationDelta(t *testing.T) {
	after := map[string]histogramSnapshot{
		"prepare": {
			Count: 4,
			Sum:   0.71,
			Buckets: map[float64]uint64{
				0.1: 2,
				0.5: 3,
				1.0: 4,
			},
		},
	}
	report := outboxStageDurationDelta(nil, after)["prepare"]
	if report.Count != 4 || report.AverageMS != 177.5 || report.P50BucketMS != 100 || report.P95BucketMS != 1000 || report.P99BucketMS != 1000 {
		t.Fatalf("stage report = %#v", report)
	}
}

func TestMetricPeakSamplerObserveKeepsMaxima(t *testing.T) {
	sampler := &metricPeakSampler{peaks: make(map[string]float64)}
	sampler.observe(map[string]float64{
		"im_backend_outbox_pending_events":             7,
		"im_backend_outbox_oldest_pending_age_seconds": 0.5,
	})
	sampler.observe(map[string]float64{
		"im_backend_outbox_pending_events":             3,
		"im_backend_outbox_oldest_pending_age_seconds": 1.25,
	})
	if sampler.samples != 2 || sampler.peaks["im_backend_outbox_pending_events"] != 7 || sampler.peaks["im_backend_outbox_oldest_pending_age_seconds"] != 1.25 {
		t.Fatalf("sampler = samples %d peaks %#v", sampler.samples, sampler.peaks)
	}
}

func TestReportKeepsLoadConditions(t *testing.T) {
	reportPayload := report{
		Concurrency:    20,
		LoadModel:      "fixed-rate",
		TargetRateRPS:  500,
		DroppedStarts:  3,
		RequestTimeout: "10s",
		DeliveryWait:   "30s",
		LoadDurationMS: 750.5,
		MetricSampling: metricSamplingReport{Interval: "250ms", Samples: 4},
	}
	payload, err := json.Marshal(reportPayload)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"concurrency":20`, `"loadModel":"fixed-rate"`, `"targetRateRps":500`, `"droppedStarts":3`, `"requestTimeout":"10s"`, `"deliveryWait":"30s"`, `"loadDurationMs":750.5`, `"interval":"250ms"`, `"samples":4`} {
		if !strings.Contains(string(payload), field) {
			t.Fatalf("report does not contain %s: %s", field, payload)
		}
	}
}

func TestFixedRateDropsStartsWhenMaxInFlightIsFull(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		var request struct {
			ClientMessageID string `json:"clientMessageId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(apiMessage{
			ID:              1,
			ClientMessageID: request.ClientMessageID,
			SenderID:        1,
			ReceiverID:      2,
		})
	}))
	defer server.Close()

	users := []testUser{
		{ID: 1, Sessions: []authResponse{{AccessToken: "sender-token"}}},
		{ID: 2, Sessions: []authResponse{{AccessToken: "receiver-token"}}},
	}
	results := sendLoad(context.Background(), server.Client(), config{
		baseURL:        server.URL,
		users:          2,
		devicesPerUser: 1,
		messages:       10,
		concurrency:    1,
		targetRate:     100,
	}, "260830120000abcdef", users, newTracker())

	started := 0
	dropped := 0
	for _, result := range results {
		if result.dropped {
			dropped++
		} else {
			started++
		}
	}
	if len(results) != 10 || started != 1 || dropped != 9 {
		t.Fatalf("results=%d started=%d dropped=%d", len(results), started, dropped)
	}
}

func TestToWebSocketURL(t *testing.T) {
	got, err := toWebSocketURL("https://example.test/api/")
	if err != nil {
		t.Fatal(err)
	}
	if got != "wss://example.test/api/ws" {
		t.Fatalf("URL = %q", got)
	}
}

func TestSendLoadRetriesUnknownOutcomeWithSameClientMessageID(t *testing.T) {
	var calls atomic.Int32
	var firstClientID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ClientMessageID string `json:"clientMessageId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if calls.Add(1) == 1 {
			firstClientID = request.ClientMessageID
			http.Error(w, `{"error":"unknown outcome"}`, http.StatusInternalServerError)
			return
		}
		if request.ClientMessageID != firstClientID {
			t.Errorf("retry clientMessageId = %q, want %q", request.ClientMessageID, firstClientID)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(apiMessage{
			ID:              7,
			ClientMessageID: request.ClientMessageID,
			SenderID:        1,
			ReceiverID:      2,
		})
	}))
	defer server.Close()

	users := []testUser{
		{ID: 1, Sessions: []authResponse{{AccessToken: "sender-token"}}},
		{ID: 2, Sessions: []authResponse{{AccessToken: "receiver-token"}}},
	}
	results := sendLoad(context.Background(), server.Client(), config{
		baseURL:        server.URL,
		users:          2,
		devicesPerUser: 1,
		messages:       1,
		concurrency:    1,
	}, "260830120000abcdef", users, newTracker())

	if calls.Load() != 2 || len(results) != 1 || results[0].err != "" || !results[0].recovered || results[0].attempts != 2 {
		t.Fatalf("calls=%d results=%+v", calls.Load(), results)
	}
}
