package utils

import (
	"os"
)

// PathExist check if the directory or file exists.
func PathExist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
