package validator

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

type LogEntry struct {
	Reason      string
	ErrorDetail error
}

type Reporter struct {
	mu         sync.Mutex
	entries    []LogEntry
	outputPath string
}

func NewReporter(outputPath string) (*Reporter, error) {
	outputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path of output path: %w", err)
	}
	return &Reporter{
		outputPath: outputPath,
	}, nil
}

func (r *Reporter) Record(reason string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, LogEntry{
		Reason:      reason,
		ErrorDetail: err,
	})
}

func (r *Reporter) Flush() {
	if len(r.entries) <= 0 {
		return
	}

	slog.Info("Start writing error report to file:", slog.String("OutputPath", r.outputPath), slog.Int("ErrorCount", len(r.entries)))
	r.mu.Lock()
	defer r.mu.Unlock()

	file, err := os.Create(r.outputPath)
	if err != nil {
		slog.Error("Failed to create error report file:", slog.String("OutputPath", r.outputPath), slog.Any("Error", err))
		return
	}
	defer file.Close()

	for _, entry := range r.entries {
		file.WriteString(fmt.Sprintf("[%s] %s\n", entry.Reason, entry.ErrorDetail.Error()))
	}

	slog.Info("Finish writing error report to file:", slog.String("OutputPath", r.outputPath))
	return
}
