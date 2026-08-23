// Package platform holds configuration shared by every component check.
package platform

import (
	"fmt"
	"os"
)

// Config is resolved entirely from the environment so the same binary runs
// against the local stack, a container in the compose network, or real AWS.
// Nothing here is component-specific beyond an address.
type Config struct {
	AWSEndpoint  string // empty means "use real AWS"
	AWSRegion    string
	Bucket       string
	TopicName    string
	QueueName    string
	SESSender    string
	KafkaBrokers string
	KafkaTopic   string
	RabbitURL    string
	DatabaseURL  string
	OTLPEndpoint string
	ServiceName  string
}

// Load reads configuration, applying defaults that match local/docker-compose.yml.
//
// Targeting real AWS requires MLP_USE_REAL_AWS=1 and nothing else. In
// particular an empty or unset AWS_ENDPOINT_URL means "use the local
// emulator", never "use real AWS" -- so a stray `export AWS_ENDPOINT_URL=`
// cannot silently point these checks at a live account.
func Load() Config {
	endpoint := env("AWS_ENDPOINT_URL", "http://localhost:4566")
	if os.Getenv("MLP_USE_REAL_AWS") == "1" {
		endpoint = ""
	}

	return Config{
		AWSEndpoint:  endpoint,
		AWSRegion:    env("AWS_DEFAULT_REGION", "us-east-1"),
		Bucket:       env("MLP_BUCKET", "mlp-artifacts"),
		TopicName:    env("MLP_TOPIC", "mlp-events"),
		QueueName:    env("MLP_QUEUE", "mlp-events-q"),
		SESSender:    env("MLP_SES_SENDER", "platform@localhost.test"),
		KafkaBrokers: env("KAFKA_BOOTSTRAP", "localhost:9092"),
		KafkaTopic:   env("MLP_KAFKA_TOPIC", "mlp.events"),
		RabbitURL:    env("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		DatabaseURL:  env("DATABASE_URL", "postgres://platform:platform@localhost:5432/platform?sslmode=disable"),
		OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
		ServiceName:  env("OTEL_SERVICE_NAME", "smoke"),
	}
}

// UsingRealAWS reports whether calls will hit the real AWS API rather than the
// local emulator. Guards anything that would cost money or send real email.
func (c Config) UsingRealAWS() bool { return c.AWSEndpoint == "" }

func (c Config) String() string {
	target := c.AWSEndpoint
	if target == "" {
		target = "REAL AWS"
	}
	return fmt.Sprintf("aws=%s region=%s kafka=%s", target, c.AWSRegion, c.KafkaBrokers)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
