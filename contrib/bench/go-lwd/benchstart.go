package cmd

import (
	"os"
	"strconv"
)

// firstHeightFromEnv returns the height at which the block cache and ingestor
// should start, read from LWD_FIRST_HEIGHT. The benchmark harness uses this to
// populate a fixed height window without syncing from genesis (stock lightwalletd
// anchors the cache at height 0). Defaults to 0 when unset or unparseable.
func firstHeightFromEnv() int {
	if v := os.Getenv("LWD_FIRST_HEIGHT"); v != "" {
		if h, err := strconv.Atoi(v); err == nil {
			return h
		}
	}
	return 0
}
