package goll

import (
	"fmt"
	"log"
)

// Logger interface is provided
// to allow you to customize the logging internally done
// by the limiters.
//
// The default implementation logs to the "log" standard module
// via log.Default().
//
// If you want to disable the default logger
// you can pass an instance of goll.NewNoOpLogger()
// to the limiter constructor.
type Logger interface {
	Debug(string)
	Info(string)
	Warning(string)
	Error(string)
}

type defaultLogger struct {
}

func (l *defaultLogger) Debug(text string) {
	log.Default().Println(fmt.Sprintf("[debug] %v", text))
}
func (l *defaultLogger) Info(text string) {
	log.Default().Println(fmt.Sprintf("[info] %v", text))
}
func (l *defaultLogger) Warning(text string) {
	log.Default().Println(fmt.Sprintf("[WARNING] %v", text))
}
func (l *defaultLogger) Error(text string) {
	log.Default().Println(fmt.Sprintf("[ERROR] %v", text))
}

func NewNoOpLogger() Logger {
	return &noOpLogger{}
}

type noOpLogger struct {
}

func (l *noOpLogger) Debug(text string) {
	// NOP
}
func (l *noOpLogger) Info(text string) {
	// NOP
}
func (l *noOpLogger) Warning(text string) {
	// NOP
}
func (l *noOpLogger) Error(text string) {
	// NOP
}
