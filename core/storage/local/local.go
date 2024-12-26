package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Storage implements Storage using the local filesystem.
type Storage struct {
	pendingDir    string
	publishingDir string
}

func NewLocalStorage(pendingDir, publishingDir string) *Storage {
	return &Storage{
		pendingDir:    pendingDir,
		publishingDir: publishingDir,
	}
}

func (s *Storage) Create(_ context.Context, path string) (io.WriteCloser, error) {
	newPath := filepath.Join(s.pendingDir, filepath.Base(path))
	file, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}

func (s *Storage) Publish(_ context.Context, path string) error {
	newPath := filepath.Join(s.publishingDir, filepath.Base(path))
	return os.Rename(path, newPath)
}

func (s *Storage) List(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.pendingDir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() {
			files = append(files, entry.Name())
		}
	}
	return files, nil
}
