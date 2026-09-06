package kafkatransport

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go/sasl"
)

const oauthBearerName = "OAUTHBEARER"

type tokenGenerator func(context.Context, string) (string, int64, error)

type mskIAMMechanism struct {
	region   string
	generate tokenGenerator
	now      func() time.Time
}

func newMSKIAMMechanism(ctx context.Context, region string) (sasl.Mechanism, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load ambient AWS configuration: %w", err)
	}
	return &mskIAMMechanism{
		region: region,
		generate: func(ctx context.Context, region string) (string, int64, error) {
			return signer.GenerateAuthTokenFromCredentialsProvider(ctx, region, cfg.Credentials)
		},
		now: time.Now,
	}, nil
}

func (m *mskIAMMechanism) Name() string { return oauthBearerName }

// Start is invoked by kafka-go for every new broker connection. Generating the
// token here, rather than when Connection is constructed, makes every new
// connection use current ambient AWS identity and a fresh signed token.
func (m *mskIAMMechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	token, expiresAtMillis, err := m.generate(ctx, m.region)
	if err != nil {
		return nil, nil, fmt.Errorf("generate MSK IAM token: %w", err)
	}
	if token == "" {
		return nil, nil, fmt.Errorf("generate MSK IAM token: signer returned an empty token")
	}
	if expiresAtMillis <= m.now().UnixMilli() {
		return nil, nil, fmt.Errorf("validate MSK IAM token expiry: signer returned an expired token")
	}

	initial := make([]byte, 0, len(token)+18)
	initial = append(initial, "n,,\x01auth=Bearer "...)
	initial = append(initial, token...)
	initial = append(initial, '\x01', '\x01')
	return oauthBearerSession{}, initial, nil
}

type oauthBearerSession struct{}

func (oauthBearerSession) Next(context.Context, []byte) (bool, []byte, error) {
	// Kafka returns the authentication failure itself. Repeating a broker
	// challenge here could expose broker-provided detail that includes the
	// signed token, so the adapter only reports success through this boundary.
	return true, nil, nil
}
