package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupTest(t *testing.T) (pendingDir, publishingDir string, cleanup func()) {
	t.Helper()

	pendingDir = t.TempDir()
	publishingDir = t.TempDir()

	cleanup = func() {
		os.RemoveAll(pendingDir)
		os.RemoveAll(publishingDir)
	}

	return pendingDir, publishingDir, cleanup
}

func TestStorage_Create(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		content string
		wantErr bool
	}{
		{
			name:    "valid file creation",
			path:    "test.txt",
			content: "hello world",
			wantErr: false,
		},
		{
			name:    "empty file creation",
			path:    "empty.txt",
			content: "",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingDir, publishingDir, cleanup := setupTest(t)
			defer cleanup()

			s := NewLocalStorage(pendingDir, publishingDir)
			w, err := s.Create(context.Background(), tt.path)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			_, err = io.WriteString(w, tt.content)
			assert.NoError(t, err)
			assert.NoError(t, w.Close())

			// Verify file exists and content
			content, err := os.ReadFile(filepath.Join(pendingDir, filepath.Base(tt.path)))
			assert.NoError(t, err)
			assert.Equal(t, tt.content, string(content))
		})
	}
}

func TestStorage_Publish(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		setup   func(string) error
		wantErr bool
	}{
		{
			name: "valid file publish",
			path: "test.txt",
			setup: func(dir string) error {
				return os.WriteFile(filepath.Join(dir, "test.txt"), []byte("content"), 0o600)
			},
			wantErr: false,
		},
		{
			name:    "non-existent file",
			path:    "nonexistent.txt",
			setup:   func(string) error { return nil },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingDir, publishingDir, cleanup := setupTest(t)
			defer cleanup()

			assert.NoError(t, tt.setup(pendingDir))

			s := NewLocalStorage(pendingDir, publishingDir)
			err := s.Publish(context.Background(), filepath.Join(pendingDir, tt.path))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			// Verify file moved to publishing dir
			_, err = os.Stat(filepath.Join(publishingDir, filepath.Base(tt.path)))
			assert.NoError(t, err)
		})
	}
}

func TestStorage_List(t *testing.T) {
	tests := []struct {
		name    string
		files   []string
		wantLen int
		wantErr bool
	}{
		{
			name:    "empty directory",
			files:   []string{},
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "multiple files",
			files:   []string{"file1.txt", "file2.txt"},
			wantLen: 2,
			wantErr: false,
		},
		{
			name:    "with subdirectory",
			files:   []string{"file1.txt", "subdir/"},
			wantLen: 1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingDir, publishingDir, cleanup := setupTest(t)
			defer cleanup()

			// Setup files
			for _, f := range tt.files {
				if filepath.Ext(f) == "" {
					assert.NoError(t, os.MkdirAll(filepath.Join(pendingDir, f), 0o600))
				} else {
					assert.NoError(t, os.WriteFile(filepath.Join(pendingDir, f), []byte("content"), 0o600))
				}
			}

			s := NewLocalStorage(pendingDir, publishingDir)
			files, err := s.List(context.Background())

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Len(t, files, tt.wantLen)
		})
	}
}

func TestStorage_Open(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		content string
		setup   func(string) error
		wantErr bool
	}{
		{
			name:    "valid file open",
			path:    "test.txt",
			content: "hello world",
			setup: func(dir string) error {
				return os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello world"), 0o600)
			},
			wantErr: false,
		},
		{
			name:    "non-existent file",
			path:    "nonexistent.txt",
			content: "",
			setup:   func(string) error { return nil },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingDir, publishingDir, cleanup := setupTest(t)
			defer cleanup()

			assert.NoError(t, tt.setup(publishingDir))

			s := NewLocalStorage(pendingDir, publishingDir)
			reader, err := s.Open(context.Background(), tt.path)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			defer reader.Close()

			// Test ReadAt functionality
			buf := make([]byte, 5)
			n, err := reader.ReadAt(buf, 0)
			assert.NoError(t, err)
			assert.Equal(t, 5, n)
			assert.Equal(t, tt.content[:5], string(buf))

			// Test reading from middle of file
			n, err = reader.ReadAt(buf, 6)
			assert.NoError(t, err)
			assert.Equal(t, 5, n)
			assert.Equal(t, tt.content[6:11], string(buf))

			// Test reading past EOF
			buf = make([]byte, 20)
			n, err = reader.ReadAt(buf, 0)
			assert.ErrorIs(t, err, io.EOF)
			assert.Equal(t, len(tt.content), n)
			assert.Equal(t, tt.content, string(buf[:n]))
		})
	}
}

func TestStorage_ListPublished(t *testing.T) {
	tests := []struct {
		name    string
		files   []string
		wantLen int
		wantErr bool
	}{
		{
			name:    "empty directory",
			files:   []string{},
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "multiple files",
			files:   []string{"file1.txt", "file2.txt"},
			wantLen: 2,
			wantErr: false,
		},
		{
			name:    "with subdirectory",
			files:   []string{"file1.txt", "subdir/"},
			wantLen: 1,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingDir, publishingDir, cleanup := setupTest(t)
			defer cleanup()

			// Setup files
			for _, f := range tt.files {
				if filepath.Ext(f) == "" {
					assert.NoError(t, os.MkdirAll(filepath.Join(publishingDir, f), 0o600))
				} else {
					assert.NoError(t, os.WriteFile(filepath.Join(publishingDir, f), []byte("content"), 0o600))
				}
			}

			s := NewLocalStorage(pendingDir, publishingDir)
			files, err := s.ListPublished(context.Background())

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Len(t, files, tt.wantLen)
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		setup   func(string) error
		wantErr bool
	}{
		{
			name: "valid file deletion",
			path: "test.txt",
			setup: func(dir string) error {
				return os.WriteFile(filepath.Join(dir, "test.txt"), []byte("content"), 0o600)
			},
			wantErr: false,
		},
		{
			name:    "non-existent file",
			path:    "nonexistent.txt",
			setup:   func(string) error { return nil },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingDir, publishingDir, cleanup := setupTest(t)
			defer cleanup()

			assert.NoError(t, tt.setup(publishingDir))

			s := NewLocalStorage(pendingDir, publishingDir)
			err := s.Delete(context.Background(), tt.path)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			// Verify file no longer exists
			_, err = os.Stat(filepath.Join(publishingDir, filepath.Base(tt.path)))
			assert.True(t, os.IsNotExist(err))
		})
	}
}
