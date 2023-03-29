package internal

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

func NewLazySSM() *LazySSM {
	return &LazySSM{}
}

type LazySSM struct {
	ssm *ssm.Client
}

func (l *LazySSM) GetValue(ctx context.Context, name string) (string, error) {
	if l.ssm == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load AWS SDK config: %w", err)
		}

		l.ssm = ssm.NewFromConfig(cfg)
	}

	param, err := l.ssm.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(name),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return "", fmt.Errorf("error response from AWS SSM: %w", err)
	}

	return *param.Parameter.Value, nil
}
