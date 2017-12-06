package analytics_test

import (
	"os"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/matthewmueller/firehose-analytics"
)

func sesh(t *testing.T) *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		),
		Region: aws.String(os.Getenv("AWS_REGION")),
	})
	if err != nil {
		t.Fatal(err)
	}
	return sess
}

func TestAnalytics(t *testing.T) {
	a := analytics.New(&analytics.Config{
		Prefix:  "app:",
		Session: sesh(t),
		Stream:  os.Getenv("FIREHOSE_STREAM_NAME"),
		Log:     log.Log,
	})

	if err := a.Track("cool", a.Body("very", "nice")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	log.Infof("time: %s", time.Since(start))
}
