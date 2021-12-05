package goll

import (
	"testing"
)

func TestDefaultLogger(t *testing.T) {
	instance := defaultLogger{}

	// just please don't... panic? idk
	strings := []string{
		"some message",
		"",
		"       ",
	}

	for _, message := range strings {
		instance.Debug(message)
		instance.Info(message)
		instance.Warning(message)
		instance.Error(message)
	}
}

func TestNoOpLogger(t *testing.T) {
	instance := NewNoOpLogger()

	// just please don't... panic? idk
	strings := []string{
		"some message",
		"",
		"       ",
	}

	for _, message := range strings {
		instance.Debug(message)
		instance.Info(message)
		instance.Warning(message)
		instance.Error(message)
	}
}
