package xaws

import (
	"context"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// LoadConfig extracts common AWS parameters from u, loads the default AWS
// config with appropriate overrides, and returns it.
//
// The endpoint is constructed from u.Host and the "insecure" query parameter:
// if u.Host is non-empty the endpoint is "https://<host>" by default, or
// "http://<host>" when the "insecure" parameter is present. It is an error to
// use "insecure" without a host. If u.Host is empty, the AWS SDK's default
// regional endpoint resolution is used.
func LoadConfig(ctx context.Context, u *url.URL) (aws.Config, error) {
	var (
		endpoint        *url.URL
		region, roleARN string
	)

	if u.Host != "" {
		endpoint = &url.URL{
			Scheme: "https",
			Host:   u.Host,
		}
	}

	q := u.Query()
	for k := range q {
		switch k {
		case "region":
			region = q.Get("region")
		case "role_arn":
			roleARN = q.Get("role_arn")
		case "insecure":
			if endpoint == nil {
				return aws.Config{}, fmt.Errorf("invalid %s URL: insecure has no effect without a host", u.Scheme)
			}
			endpoint.Scheme = "http"
		default:
			return aws.Config{}, fmt.Errorf("invalid %s URL: unknown parameter %q", u.Scheme, k)
		}
	}

	var opts []func(*config.LoadOptions) error

	if endpoint != nil {
		opts = append(opts, config.WithBaseEndpoint(endpoint.String()))
	}

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("could not load AWS config: %w", err)
	}

	if roleARN != "" {
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(stsClient, roleARN),
		)
	}

	return cfg, nil
}
