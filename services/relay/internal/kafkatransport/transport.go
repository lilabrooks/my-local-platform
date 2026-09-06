// Package kafkatransport owns relay's Kafka connection policy.
package kafkatransport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	AuthNone   = "none"
	AuthMSKIAM = "aws_msk_iam"
)

// Connection is the single broker and authentication configuration shared by
// relay's readers, writers, lag poller, and replay command.
type Connection struct {
	brokers   []string
	authMode  string
	dialer    *kafka.Dialer
	transport *kafka.Transport
}

// New validates the runtime connection settings. Local mode deliberately
// leaves Dialer and Transport nil so kafka-go retains its unauthenticated TCP
// defaults. MSK IAM mode always enables certificate and server-name checks.
func New(ctx context.Context, bootstrap, authMode, region string) (*Connection, error) {
	brokers, err := parseBrokers(bootstrap)
	if err != nil {
		return nil, err
	}

	authMode = strings.TrimSpace(authMode)
	if authMode == "" {
		authMode = AuthNone
	}
	c := &Connection{brokers: brokers, authMode: authMode}
	switch authMode {
	case AuthNone:
		return c, nil
	case AuthMSKIAM:
		if strings.TrimSpace(region) == "" {
			return nil, fmt.Errorf("AWS_REGION is required when KAFKA_AUTH_MODE=%s", AuthMSKIAM)
		}
		mechanism, err := newMSKIAMMechanism(ctx, strings.TrimSpace(region))
		if err != nil {
			return nil, err
		}
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		c.dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			TLS:           tlsConfig,
			SASLMechanism: mechanism,
		}
		c.transport = &kafka.Transport{
			DialTimeout: 10 * time.Second,
			TLS:         tlsConfig,
			SASL:        mechanism,
		}
		return c, nil
	default:
		return nil, fmt.Errorf("KAFKA_AUTH_MODE %q is not one of: %s, %s", authMode, AuthNone, AuthMSKIAM)
	}
}

func parseBrokers(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			return nil, fmt.Errorf("KAFKA_BOOTSTRAP contains an empty broker address")
		}
		if _, _, err := net.SplitHostPort(broker); err != nil {
			return nil, fmt.Errorf("KAFKA_BOOTSTRAP broker %q: %w", broker, err)
		}
		brokers = append(brokers, broker)
	}
	return brokers, nil
}

func (c *Connection) Brokers() []string { return append([]string(nil), c.brokers...) }
func (c *Connection) Addr() net.Addr    { return kafka.TCP(c.brokers...) }
func (c *Connection) AuthMode() string  { return c.authMode }
func (c *Connection) Dialer() *kafka.Dialer {
	return c.dialer
}
func (c *Connection) RoundTripper() kafka.RoundTripper {
	if c.transport == nil {
		return nil
	}
	return c.transport
}

// Close retires connections held by the shared authenticated transport.
func (c *Connection) Close() error {
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	return nil
}
