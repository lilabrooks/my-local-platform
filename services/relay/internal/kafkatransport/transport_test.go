package kafkatransport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/saslauthenticate"
	"github.com/segmentio/kafka-go/protocol/saslhandshake"
)

func TestMSKIAMMechanismInitialResponse(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	m := &mskIAMMechanism{
		region: "us-east-1",
		now:    func() time.Time { return now },
		generate: func(_ context.Context, region string) (string, int64, error) {
			if region != "us-east-1" {
				t.Fatalf("region = %q", region)
			}
			return "signed-token", now.Add(15 * time.Minute).UnixMilli(), nil
		},
	}

	if got := m.Name(); got != "OAUTHBEARER" {
		t.Fatalf("Name() = %q", got)
	}
	session, initial, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(initial), "n,,\x01auth=Bearer signed-token\x01\x01"; got != want {
		t.Fatalf("initial response = %q, want %q", got, want)
	}
	done, response, err := session.Next(context.Background(), nil)
	if err != nil || !done || response != nil {
		t.Fatalf("Next() = (%v, %q, %v)", done, response, err)
	}
}

func TestMSKIAMMechanismRefreshesPerStart(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	calls := 0
	m := &mskIAMMechanism{
		region: "us-east-1",
		now:    func() time.Time { return now },
		generate: func(context.Context, string) (string, int64, error) {
			calls++
			return "token-" + string(rune('0'+calls)), now.Add(15 * time.Minute).UnixMilli(), nil
		},
	}

	_, first, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := m.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("generator calls = %d, want 2", calls)
	}
	if string(first) == string(second) {
		t.Fatalf("two connections reused initial response %q", first)
	}
}

func TestMSKIAMMechanismRejectsExpiredTokenWithoutLeakingIt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const secret = "do-not-log-this-token"
	m := &mskIAMMechanism{
		region: "us-east-1",
		now:    func() time.Time { return now },
		generate: func(context.Context, string) (string, int64, error) {
			return secret, now.UnixMilli(), nil
		},
	}

	_, _, err := m.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "expiry") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked token: %v", err)
	}
}

func TestMSKIAMMechanismNamesTokenGenerationStage(t *testing.T) {
	m := &mskIAMMechanism{
		region: "us-east-1",
		now:    time.Now,
		generate: func(context.Context, string) (string, int64, error) {
			return "", 0, errors.New("ambient identity unavailable")
		},
	}

	_, _, err := m.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "generate MSK IAM token") {
		t.Fatalf("error = %v", err)
	}
}

func TestConnectionModes(t *testing.T) {
	local, err := New("kafka-a:9092, kafka-b:9092", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if local.AuthMode() != AuthNone || local.Dialer() != nil || local.RoundTripper() != nil {
		t.Fatalf("local mode enabled authentication: %#v", local)
	}
	if got := local.Brokers(); len(got) != 2 || got[0] != "kafka-a:9092" || got[1] != "kafka-b:9092" {
		t.Fatalf("brokers = %v", got)
	}

	iam, err := New("broker.example:9098", AuthMSKIAM, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	if iam.Dialer() == nil || iam.RoundTripper() == nil {
		t.Fatal("IAM mode did not configure both dial paths")
	}
	if iam.dialer.TLS.InsecureSkipVerify || iam.transport.TLS.InsecureSkipVerify {
		t.Fatal("IAM mode disabled certificate verification")
	}
	if iam.dialer.TLS.ServerName != "" || iam.transport.TLS.ServerName != "" {
		t.Fatal("server name must remain empty for kafka-go to infer it per broker")
	}
	if iam.dialer.TLS.MinVersion < tls.VersionTLS12 || iam.transport.TLS.MinVersion < tls.VersionTLS12 {
		t.Fatal("IAM mode permits TLS older than 1.2")
	}
	if iam.dialer.SASLMechanism.Name() != "OAUTHBEARER" || iam.transport.SASL.Name() != "OAUTHBEARER" {
		t.Fatal("IAM mode did not configure OAUTHBEARER on both dial paths")
	}
}

func TestMSKIAMMechanismCompletesKafkaProtocolHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	now := time.Unix(1_700_000_000, 0)
	mechanism := &mskIAMMechanism{
		region: "us-east-1",
		now:    func() time.Time { return now },
		generate: func(context.Context, string) (string, int64, error) {
			return "fake-signed-token", now.Add(15 * time.Minute).UnixMilli(), nil
		},
	}
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serveFakeOAUTHBEARER(listener, []byte("n,,\x01auth=Bearer fake-signed-token\x01\x01"))
	}()

	transport := &kafka.Transport{SASL: mechanism, DialTimeout: 2 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	client := &kafka.Client{Addr: kafka.TCP(listener.Addr().String()), Transport: transport, Timeout: 2 * time.Second}
	if _, err := client.Metadata(context.Background(), &kafka.MetadataRequest{Topics: []string{"events"}}); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func serveFakeOAUTHBEARER(listener net.Listener, wantInitial []byte) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	version, correlation, _, request, err := protocol.ReadRequest(conn)
	if err != nil {
		return fmt.Errorf("read ApiVersions: %w", err)
	}
	if _, ok := request.(*apiversions.Request); !ok {
		return fmt.Errorf("first request is %T, want ApiVersions", request)
	}
	if err := protocol.WriteResponse(conn, version, correlation, &apiversions.Response{ApiKeys: []apiversions.ApiKeyResponse{
		{ApiKey: int16(protocol.SaslHandshake), MinVersion: 1, MaxVersion: 1},
		{ApiKey: int16(protocol.SaslAuthenticate), MinVersion: 1, MaxVersion: 1},
		{ApiKey: int16(protocol.Metadata), MinVersion: 0, MaxVersion: 0},
	}}); err != nil {
		return fmt.Errorf("write ApiVersions: %w", err)
	}

	version, correlation, _, request, err = protocol.ReadRequest(conn)
	if err != nil {
		return fmt.Errorf("read SaslHandshake: %w", err)
	}
	handshake, ok := request.(*saslhandshake.Request)
	if !ok || handshake.Mechanism != "OAUTHBEARER" {
		return fmt.Errorf("handshake = %#v, want OAUTHBEARER", request)
	}
	if err := protocol.WriteResponse(conn, version, correlation, &saslhandshake.Response{
		Mechanisms: []string{"OAUTHBEARER"},
	}); err != nil {
		return fmt.Errorf("write SaslHandshake: %w", err)
	}

	version, correlation, _, request, err = protocol.ReadRequest(conn)
	if err != nil {
		return fmt.Errorf("read SaslAuthenticate: %w", err)
	}
	authenticate, ok := request.(*saslauthenticate.Request)
	if !ok || string(authenticate.AuthBytes) != string(wantInitial) {
		return fmt.Errorf("authentication bytes = %q, want %q", authenticate.AuthBytes, wantInitial)
	}
	if err := protocol.WriteResponse(conn, version, correlation, &saslauthenticate.Response{}); err != nil {
		return fmt.Errorf("write SaslAuthenticate: %w", err)
	}

	version, correlation, _, request, err = protocol.ReadRequest(conn)
	if err != nil {
		return fmt.Errorf("read Metadata: %w", err)
	}
	if _, ok := request.(*metadata.Request); !ok {
		return fmt.Errorf("request after authentication is %T, want Metadata", request)
	}
	return protocol.WriteResponse(conn, version, correlation, &metadata.Response{Topics: []metadata.ResponseTopic{{Name: "events"}}})
}

func TestConnectionValidation(t *testing.T) {
	tests := []struct {
		name, brokers, mode, region, want string
	}{
		{"empty broker", "broker:9092,", AuthNone, "", "empty broker"},
		{"missing port", "broker", AuthNone, "", "missing port"},
		{"unknown mode", "broker:9092", "plain", "", "not one of"},
		{"missing region", "broker:9098", AuthMSKIAM, "", "AWS_REGION"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.brokers, tt.mode, tt.region)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}
