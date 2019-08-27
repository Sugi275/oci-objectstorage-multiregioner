package loglib

import (
	"go.uber.org/zap"
)

// Sugar logging object
var Sugar *zap.SugaredLogger

// InitSugar InitSugar
func InitSugar() {
	if Sugar == nil {
		logger, _ := zap.NewDevelopment()
		Sugar = logger.Sugar()
	}
}