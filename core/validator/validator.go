package validator

import (
	"context"
)

type Validator interface {
	Validate(ctx context.Context, filePath string, workerCount int) error
}
