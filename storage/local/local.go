package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/davidvella/xp/consumer"
)

// Storage implements Storage and Storage using the local filesystem.
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
	file, err := os.OpenFile(filepath.Join(s.pendingDir, filepath.Base(path)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
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

// Open opens a published file for reading.
func (s *Storage) Open(_ context.Context, path string) (consumer.ReadAtCloser, error) {
	file, err := os.Open(filepath.Join(s.publishingDir, filepath.Base(path)))
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}

// ListPublished lists all files in the publishing directory.
func (s *Storage) ListPublished(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.publishingDir)
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

// Delete removes a published file.
func (s *Storage) Delete(_ context.Context, path string) error {
	err := os.Remove(filepath.Join(s.publishingDir, filepath.Base(path)))
	if err != nil {
		return fmt.Errorf("failed to delete file %s: %w", path, err)
	}
	return nil
}
