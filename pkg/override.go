package pkg

import (
	"os"
	"strconv"
	"time"
)

// OverrideWithStringEnvVar Helper functions (could be moved to a shared package)
func OverrideWithStringEnvVar(envKey string, value *string) {
	if envVal, exists := os.LookupEnv(envKey); exists {
		*value = envVal
	}
}
func OverrideWithIntEnvVar(envKey string, value *int) {
	if envValStr, exists := os.LookupEnv(envKey); exists {
		if intVal, err := strconv.Atoi(envValStr); err == nil {
			*value = intVal
		}
	}
}
func OverrideWithDurationEnvVar(envKey string, value *time.Duration) {
	if envValStr, exists := os.LookupEnv(envKey); exists {
		if durVal, err := time.ParseDuration(envValStr); err == nil {
			*value = durVal
		}
	}
}
