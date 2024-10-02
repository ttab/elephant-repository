package repository

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Options struct {
	Endpoint        string
	AccessKeyID     string
	AccessKeySecret string
	DisableHTTPS    bool
	HTTPClient      *http.Client
}

func S3Client(
	ctx context.Context, opts S3Options,
) (*s3.Client, error) {
	var (
		options   []func(*config.LoadOptions) error
		s3Options []func(*s3.Options)
	)

	if opts.Endpoint != "" {
		//nolint: staticcheck
		customResolver := aws.EndpointResolverWithOptionsFunc(func(
			service, region string, _ ...interface{},
		) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           opts.Endpoint,
					SigningRegion: region,
				}, nil
			}

			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})

		//nolint: staticcheck
		options = append(options,
			config.WithEndpointResolverWithOptions(customResolver),
			config.WithRegion("auto"),
		)

		s3Options = append(s3Options, func(o *s3.Options) {
			o.EndpointOptions.DisableHTTPS = opts.DisableHTTPS
			o.UsePathStyle = true
		})
	}

	if opts.AccessKeyID != "" {
		creds := credentials.NewStaticCredentialsProvider(
			opts.AccessKeyID, opts.AccessKeySecret, "")

		options = append(options,
			config.WithCredentialsProvider(creds))
	}

	if opts.HTTPClient != nil {
		options = append(options, config.WithHTTPClient(opts.HTTPClient))
	}

	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS SDK config: %w", err)
	}

	client := s3.NewFromConfig(cfg, s3Options...)

	return client, nil
}
