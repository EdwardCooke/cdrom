package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	glogger "gorm.io/gorm/logger"
)

// gormLogger adapts a *slog.Logger to GORM's logger interface so that ORM
// diagnostics flow through the project's structured logging.
type gormLogger struct {
	logger *slog.Logger
}

func (l gormLogger) LogMode(glogger.LogLevel) glogger.Interface { return l }

func (l gormLogger) Info(_ context.Context, msg string, data ...any) {
	l.logger.Info(fmt.Sprintf("gorm: %s", msg), "data", anySlice(data))
}

func (l gormLogger) Warn(_ context.Context, msg string, data ...any) {
	l.logger.Warn(fmt.Sprintf("gorm: %s", msg), "data", anySlice(data))
}

func (l gormLogger) Error(_ context.Context, msg string, data ...any) {
	l.logger.Error(fmt.Sprintf("gorm: %s", msg), "data", anySlice(data))
}

func (l gormLogger) Trace(_ context.Context, _ time.Time, _ func() (string, int64), err error) {
	if err != nil {
		l.logger.Error(fmt.Sprintf("gorm: %s", err.Error()))
	}
}

func anySlice(data []any) any { return data }
