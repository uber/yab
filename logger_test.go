package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestLoggerConfigure(t *testing.T) {
	tests := []struct {
		options         *Options
		wantLoggerLevel zapcore.Level
	}{
		{
			options:         &Options{Verbosity: []bool{}},
			wantLoggerLevel: zapcore.WarnLevel,
		},
		{
			options:         &Options{Verbosity: []bool{true}},
			wantLoggerLevel: zapcore.InfoLevel,
		},
		{
			options:         &Options{Verbosity: []bool{true, true}},
			wantLoggerLevel: zapcore.DebugLevel,
		},
		{
			options:         &Options{Verbosity: []bool{true, true, true}},
			wantLoggerLevel: zapcore.DebugLevel,
		},
		{
			options:         &Options{Verbosity: []bool{false}},
			wantLoggerLevel: zapcore.InfoLevel,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("Test logger: %v", tt.wantLoggerLevel), func(t *testing.T) {
			lconf := configureLoggerConfig(tt.options)
			assert.Equal(t, lconf.Level.Level(), tt.wantLoggerLevel)
		})
	}
}

func TestLoggerGetLoggerVerbosity(t *testing.T) {
	tests := []struct {
		verbocityLevel  VerbosityLevel
		wantLoggerLevel zapcore.Level
	}{
		{
			verbocityLevel:  VerbosityLevelOff,
			wantLoggerLevel: zapcore.WarnLevel,
		},
		{
			verbocityLevel:  VerbosityLevelInfo,
			wantLoggerLevel: zapcore.InfoLevel,
		},
		{
			verbocityLevel:  VerbosityLevelDebug,
			wantLoggerLevel: zapcore.DebugLevel,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("get_logger_verbocity: %v", tt.verbocityLevel), func(t *testing.T) {
			lvl := getLoggerVerbosity(tt.verbocityLevel)
			assert.Equal(t, lvl, tt.wantLoggerLevel)
		})
	}
}
