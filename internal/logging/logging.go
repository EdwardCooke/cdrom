// Package logging provides centralized logging setup for all cdrom
// binaries.
//
// Every binary obtains its logger from this package so the format, level,
// and output are configured in one place. The handler is customizable:
// by default a JSON handler is used, and a raw text handler can be selected
// via the environment. Output goes to stdout by default, or to the file
// named by CDROM_LOG_FILE when that variable is set. Callers that need a
// fully custom handler can build one with NewHandler and wrap it, or pass
// their own handler to NewWithHandler.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Environment variables that control logging for every binary.
const (
	// EnvFormat selects the log format: "json" (default) or "text" for raw
	// text output.
	EnvFormat = "CDROM_LOG_FORMAT"
	// EnvLevel sets the minimum log level: debug, info (default), warn, or
	// error.
	EnvLevel = "CDROM_LOG_LEVEL"
	// EnvLogFile names the file to write logs to. When unset, logs go to
	// stdout.
	EnvLogFile = "CDROM_LOG_FILE"
)

// New creates the standard logger for a binary: configured from the
// environment (CDROM_LOG_FORMAT, CDROM_LOG_LEVEL) and writing to the
// file named by CDROM_LOG_FILE, or to stdout when that is unset. If the
// environment is misconfigured it falls back to a JSON handler on stdout so
// a bad log setting can never prevent a service from starting.
func New() *slog.Logger {
	writer, closer, err := logWriter()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).
			Error("logging: invalid configuration; falling back to JSON on stdout", "err", err)
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	handler, err := NewHandler(writer)
	if err != nil {
		_ = closer()
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).
			Error("logging: invalid configuration; falling back to JSON on stdout", "err", err)
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	return slog.New(handler)
}

// logWriter returns the writer logs should go to (the file named by
// CDROM_LOG_FILE when set, otherwise stdout) and a closer that releases
// the file handle. The closer is a no-op for stdout.
func logWriter() (io.Writer, func() error, error) {
	if path := os.Getenv(EnvLogFile); path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, nil, fmt.Errorf("logging: open %s: %w", path, err)
		}
		return f, f.Close, nil
	}
	return os.Stdout, func() error { return nil }, nil
}

// NewHandler builds a slog.Handler from the environment (CDROM_LOG_FORMAT
// selects json or text; CDROM_LOG_LEVEL sets the minimum level) that
// writes to w. Use this when a custom writer is needed, e.g. a file.
func NewHandler(w io.Writer) (slog.Handler, error) {
	options := &slog.HandlerOptions{}
	if level := os.Getenv(EnvLevel); level != "" {
		parsed, err := parseLevel(level)
		if err != nil {
			return nil, err
		}
		options.Level = parsed
	}
	switch strings.ToLower(os.Getenv(EnvFormat)) {
	case "", "json":
		return slog.NewJSONHandler(w, options), nil
	case "text":
		return slog.NewTextHandler(w, options), nil
	default:
		return nil, fmt.Errorf("logging: unknown %s %q (want json or text)", EnvFormat, os.Getenv(EnvFormat))
	}
}

// NewWithHandler creates a logger from a fully custom handler, for callers
// that need to compose their own handler (e.g. multi-writer or sampling).
func NewWithHandler(handler slog.Handler) *slog.Logger {
	return slog.New(handler)
}

func parseLevel(value string) (slog.Level, error) {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging: unknown %s %q (want debug, info, warn, or error)", EnvLevel, value)
	}
}
