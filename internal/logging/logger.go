package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	}
	return "UNKNOWN"
}

func ParseLevel(s string) Level {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO
	}
}

type Logger struct {
	level  Level
	logger *log.Logger
	file   *lumberjack.Logger
}

func New(logFile string, level Level) *Logger {
	lj := &lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    10,
		MaxAge:     3,
		MaxBackups: 3,
		Compress:   true,
	}

	writer := io.MultiWriter(lj, os.Stderr)
	l := log.New(writer, "", log.LstdFlags|log.Lmsgprefix)

	return &Logger{
		level:  level,
		logger: l,
		file:   lj,
	}
}

func (l *Logger) Close() error {
	return l.file.Close()
}

func (l *Logger) log(level Level, component string, format string, args ...any) {
	if level < l.level {
		return
	}
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] [%s] %s", level, component, msg)
}

func (l *Logger) Debug(component string, format string, args ...any) {
	l.log(DEBUG, component, format, args...)
}

func (l *Logger) Info(component string, format string, args ...any) {
	l.log(INFO, component, format, args...)
}

func (l *Logger) Warn(component string, format string, args ...any) {
	l.log(WARN, component, format, args...)
}

func (l *Logger) Error(component string, format string, args ...any) {
	l.log(ERROR, component, format, args...)
}
