package checks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

// awsConfig builds an SDK config pointed at either the local emulator or real
// AWS. Against the emulator we pin static dummy credentials so an expired or
// absent SSO session can't turn into a confusing auth error.
func awsConfig(ctx context.Context, cfg platform.Config) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AWSRegion),
	}
	if !cfg.UsingRealAWS() {
		opts = append(opts,
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider("test", "test", ""),
			),
			awsconfig.WithBaseEndpoint(cfg.AWSEndpoint),
		)
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

// S3 writes an object, reads it back, and verifies the bytes match.
func S3(cfg platform.Config) Check {
	return Check{Name: "s3", Run: func(ctx context.Context) (string, error) {
		ac, err := awsConfig(ctx, cfg)
		if err != nil {
			return "", err
		}
		// Path style: the emulator does not do bucket-as-subdomain DNS.
		client := s3.NewFromConfig(ac, func(o *s3.Options) { o.UsePathStyle = true })

		key := fmt.Sprintf("smoke/%d.txt", time.Now().UnixNano())
		want := []byte("hello from the smoke check")

		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(want),
		}); err != nil {
			return "", fmt.Errorf("put object: %w", err)
		}

		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return "", fmt.Errorf("get object: %w", err)
		}
		defer out.Body.Close()

		got, err := io.ReadAll(out.Body)
		if err != nil {
			return "", fmt.Errorf("read body: %w", err)
		}
		if !bytes.Equal(got, want) {
			return "", fmt.Errorf("round trip mismatch: wrote %q, read %q", want, got)
		}
		return fmt.Sprintf("s3://%s/%s round trip", cfg.Bucket, key), nil
	}}
}

// SNSToSQS publishes to the topic and waits for the message to arrive on the
// subscribed queue, proving the fanout wiring rather than just the publish.
func SNSToSQS(cfg platform.Config) Check {
	return Check{Name: "sns->sqs", Run: func(ctx context.Context) (string, error) {
		ac, err := awsConfig(ctx, cfg)
		if err != nil {
			return "", err
		}
		snsClient := sns.NewFromConfig(ac)
		sqsClient := sqs.NewFromConfig(ac)

		topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
			Name: aws.String(cfg.TopicName),
		})
		if err != nil {
			return "", fmt.Errorf("resolve topic: %w", err)
		}
		queue, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(cfg.QueueName),
		})
		if err != nil {
			return "", fmt.Errorf("resolve queue: %w", err)
		}

		// Drain anything left by an earlier run so we match our own message.
		_, _ = sqsClient.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: queue.QueueUrl})

		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
		if _, err := snsClient.Publish(ctx, &sns.PublishInput{
			TopicArn: topic.TopicArn,
			Message:  aws.String(marker),
		}); err != nil {
			return "", fmt.Errorf("publish: %w", err)
		}

		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            queue.QueueUrl,
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     2,
			})
			if err != nil {
				return "", fmt.Errorf("receive: %w", err)
			}
			for _, m := range out.Messages {
				body := aws.ToString(m.Body)
				_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      queue.QueueUrl,
					ReceiptHandle: m.ReceiptHandle,
				})
				// RawMessageDelivery is on, so the body is the marker itself.
				// Fall back to a contains check in case it is ever turned off
				// and the body arrives wrapped in SNS envelope JSON.
				if body == marker || bytes.Contains([]byte(body), []byte(marker)) {
					return "fanout delivered " + marker, nil
				}
			}
		}
		return "", fmt.Errorf("message %s did not arrive on %s within 20s", marker, cfg.QueueName)
	}}
}

// SES verifies the sender identity exists and sends one message. Against real
// AWS this is skipped: SES in sandbox mode rejects unverified recipients and a
// live send is a side effect a smoke check has no business causing.
func SES(cfg platform.Config) Check {
	return Check{Name: "ses", Run: func(ctx context.Context) (string, error) {
		if cfg.UsingRealAWS() {
			return "skipped against real AWS (would send actual email)", nil
		}
		ac, err := awsConfig(ctx, cfg)
		if err != nil {
			return "", err
		}
		client := ses.NewFromConfig(ac)

		if _, err := client.VerifyEmailIdentity(ctx, &ses.VerifyEmailIdentityInput{
			EmailAddress: aws.String(cfg.SESSender),
		}); err != nil {
			return "", fmt.Errorf("verify identity: %w", err)
		}

		out, err := client.SendEmail(ctx, &ses.SendEmailInput{
			Source:      aws.String(cfg.SESSender),
			Destination: &sestypes.Destination{ToAddresses: []string{"someone@localhost.test"}},
			Message: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String("smoke check")},
				Body: &sestypes.Body{
					Text: &sestypes.Content{Data: aws.String("sent by the platform smoke check")},
				},
			},
		})
		if err != nil {
			return "", fmt.Errorf("send email: %w", err)
		}
		return "sent message " + aws.ToString(out.MessageId), nil
	}}
}
