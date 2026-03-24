package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sudokatie/epoch/internal/storage"
	"github.com/sudokatie/epoch/pkg/protocol"
)

// ImportConfig holds import configuration
type ImportConfig struct {
	Host        string
	File        string
	Database    string
	Measurement string
	Format      string
	BatchSize   int
	TagColumns  string
}

// runImport imports data from a file
func runImport(cfg ImportConfig) error {
	// Detect format if auto
	format := cfg.Format
	if format == "auto" {
		format = detectFormat(cfg.File)
	}

	// Open file
	file, err := os.Open(cfg.File)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Get file stats for progress
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	totalSize := stat.Size()

	fmt.Printf("Importing %s (%s format) to %s.%s\n", cfg.File, format, cfg.Database, cfg.Measurement)

	client := &http.Client{Timeout: 30 * time.Second}
	writeURL := fmt.Sprintf("http://%s/write?db=%s", cfg.Host, url.QueryEscape(cfg.Database))

	var pointsImported int64
	var bytesRead int64
	startTime := time.Now()

	switch format {
	case "line":
		pointsImported, bytesRead, err = importLineProtocol(file, client, writeURL, cfg.BatchSize)
	case "json":
		pointsImported, bytesRead, err = importJSON(file, client, writeURL, cfg.BatchSize)
	case "csv":
		pointsImported, bytesRead, err = importCSV(file, client, writeURL, cfg)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}

	if err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	rate := float64(pointsImported) / elapsed.Seconds()

	fmt.Printf("\nImport complete:\n")
	fmt.Printf("  Points imported: %d\n", pointsImported)
	fmt.Printf("  Bytes read:      %d / %d\n", bytesRead, totalSize)
	fmt.Printf("  Time elapsed:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Rate:            %.0f points/sec\n", rate)

	return nil
}

func detectFormat(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".json":
		return "json"
	case ".csv":
		return "csv"
	default:
		return "line"
	}
}

func importLineProtocol(r io.Reader, client *http.Client, writeURL string, batchSize int) (int64, int64, error) {
	scanner := bufio.NewScanner(r)
	// Increase buffer size for large lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var batch bytes.Buffer
	var linesInBatch int
	var totalPoints int64
	var totalBytes int64

	for scanner.Scan() {
		line := scanner.Text()
		totalBytes += int64(len(line)) + 1 // +1 for newline

		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		batch.WriteString(line)
		batch.WriteByte('\n')
		linesInBatch++

		if linesInBatch >= batchSize {
			if err := sendBatch(client, writeURL, batch.Bytes()); err != nil {
				return totalPoints, totalBytes, err
			}
			totalPoints += int64(linesInBatch)
			batch.Reset()
			linesInBatch = 0
			fmt.Printf("\rImported %d points...", totalPoints)
		}
	}

	// Send remaining batch
	if linesInBatch > 0 {
		if err := sendBatch(client, writeURL, batch.Bytes()); err != nil {
			return totalPoints, totalBytes, err
		}
		totalPoints += int64(linesInBatch)
	}

	if err := scanner.Err(); err != nil {
		return totalPoints, totalBytes, fmt.Errorf("read error: %w", err)
	}

	return totalPoints, totalBytes, nil
}

func importJSON(r io.Reader, client *http.Client, writeURL string, batchSize int) (int64, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, 0, fmt.Errorf("read error: %w", err)
	}

	points, err := protocol.ParseJSON(data)
	if err != nil {
		return 0, int64(len(data)), fmt.Errorf("parse error: %w", err)
	}

	var totalPoints int64

	// Convert to line protocol and send in batches
	var batch bytes.Buffer
	for i, p := range points {
		line := formatLineProtocol(p)
		batch.WriteString(line)
		batch.WriteByte('\n')

		if (i+1)%batchSize == 0 {
			if err := sendBatch(client, writeURL, batch.Bytes()); err != nil {
				return totalPoints, int64(len(data)), err
			}
			totalPoints += int64(batchSize)
			batch.Reset()
			fmt.Printf("\rImported %d points...", totalPoints)
		}
	}

	// Send remaining
	remaining := len(points) % batchSize
	if remaining > 0 {
		if err := sendBatch(client, writeURL, batch.Bytes()); err != nil {
			return totalPoints, int64(len(data)), err
		}
		totalPoints += int64(remaining)
	}

	return totalPoints, int64(len(data)), nil
}

func importCSV(r io.Reader, client *http.Client, writeURL string, cfg ImportConfig) (int64, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, 0, fmt.Errorf("read error: %w", err)
	}

	csvConfig := protocol.DefaultCSVConfig()
	csvConfig.Measurement = cfg.Measurement
	if cfg.TagColumns != "" {
		csvConfig.TagColumns = strings.Split(cfg.TagColumns, ",")
	}

	points, err := protocol.ParseCSV(string(data), csvConfig)
	if err != nil {
		return 0, int64(len(data)), fmt.Errorf("parse error: %w", err)
	}

	var totalPoints int64

	// Convert to line protocol and send in batches
	var batch bytes.Buffer
	for i, p := range points {
		line := formatLineProtocol(p)
		batch.WriteString(line)
		batch.WriteByte('\n')

		if (i+1)%cfg.BatchSize == 0 {
			if err := sendBatch(client, writeURL, batch.Bytes()); err != nil {
				return totalPoints, int64(len(data)), err
			}
			totalPoints += int64(cfg.BatchSize)
			batch.Reset()
			fmt.Printf("\rImported %d points...", totalPoints)
		}
	}

	// Send remaining
	remaining := len(points) % cfg.BatchSize
	if remaining > 0 {
		if err := sendBatch(client, writeURL, batch.Bytes()); err != nil {
			return totalPoints, int64(len(data)), err
		}
		totalPoints += int64(remaining)
	}

	return totalPoints, int64(len(data)), nil
}

func sendBatch(client *http.Client, url string, data []byte) error {
	resp, err := client.Post(url, "text/plain", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func formatLineProtocol(p *storage.DataPoint) string {
	var sb strings.Builder

	sb.WriteString(p.Measurement)

	// Tags
	for k, v := range p.Tags {
		sb.WriteByte(',')
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(v)
	}

	sb.WriteByte(' ')

	// Fields
	first := true
	for k, v := range p.Fields {
		if !first {
			sb.WriteByte(',')
		}
		first = false

		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(formatImportFieldValue(v))
	}

	// Timestamp
	sb.WriteByte(' ')
	sb.WriteString(fmt.Sprintf("%d", p.Timestamp))

	return sb.String()
}

func formatImportFieldValue(fv storage.FieldValue) string {
	switch fv.Type {
	case storage.FieldTypeFloat:
		return fmt.Sprintf("%v", fv.FloatValue)
	case storage.FieldTypeInteger:
		return fmt.Sprintf("%di", fv.IntValue)
	case storage.FieldTypeString:
		return fmt.Sprintf("%q", fv.StringValue)
	case storage.FieldTypeBoolean:
		if fv.BooleanValue {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}
