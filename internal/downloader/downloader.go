package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"surge/internal/utils"
)

// SingleDownloader handles single-threaded downloads
type SingleDownloader struct {
	Client       *http.Client
	ProgressChan chan<- tea.Msg // Channel for events (start/complete/error)
	ID           string         // Download ID
	State        *ProgressState // Shared state for TUI polling
	Runtime      *RuntimeConfig
}

// NewSingleDownloader creates a new single-threaded downloader with all required parameters
func NewSingleDownloader(id string, progressCh chan<- tea.Msg, state *ProgressState, runtime *RuntimeConfig) *SingleDownloader {
	return &SingleDownloader{
		Client:       &http.Client{Timeout: 0},
		ProgressChan: progressCh,
		ID:           id,
		State:        state,
		Runtime:      runtime,
	}
}

// Download downloads a file using a single connection
// Uses pre-probed metadata (file size and filename already known)
func (d *SingleDownloader) Download(ctx context.Context, rawurl, destPath string, fileSize int64, filename string, verbose bool) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", d.Runtime.GetUserAgent())

	resp, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Use .surge extension for incomplete downloads
	workingPath := destPath + IncompleteSuffix
	outFile, err := os.Create(workingPath)
	if err != nil {
		return err
	}
	closed := false

	defer func() {
		if !closed {
			outFile.Close()
		}
		if err != nil {
			os.Remove(workingPath)
		}
	}()

	start := time.Now()

	// Copy response body to file
	var written int64
	buf := make([]byte, 32*1024)

	for {
		nr, er := resp.Body.Read(buf)
		if nr > 0 {
			nw, ew := outFile.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
				if d.State != nil {
					d.State.Downloaded.Store(written)
				}
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}

	if err != nil {
		return fmt.Errorf("copy failed: %w", err)
	}

	if err = outFile.Sync(); err != nil {
		return err
	}
	if err = outFile.Close(); err != nil {
		return err
	}
	closed = true

	elapsed := time.Since(start)
	speed := float64(written) / 1024.0 / elapsed.Seconds()
	fmt.Fprintf(os.Stderr, "\nDownloaded %s in %s (%s/s)\n",
		destPath,
		elapsed.Round(time.Second),
		utils.ConvertBytesToHumanReadable(int64(speed*1024)),
	)

	// Rename .surge file to final destination
	if renameErr := os.Rename(workingPath, destPath); renameErr != nil {
		if in, rerr := os.Open(workingPath); rerr == nil {
			defer in.Close()
			if out, werr := os.Create(destPath); werr == nil {
				defer out.Close()
				if _, cerr := io.Copy(out, in); cerr != nil {
					return cerr
				}
			} else {
				return werr
			}
		} else {
			return rerr
		}
		os.Remove(workingPath)
	}

	return nil
}
