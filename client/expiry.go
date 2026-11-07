package main

import (
	"fmt"
	"time"
)

func validateExpiry(autoExpire, scavengeTTL int) error {
	maxSeconds := int64((1<<63 - 1) / time.Second)
	if autoExpire < 0 || int64(autoExpire) > maxSeconds {
		return fmt.Errorf("autoexpire must be between 0 and %d seconds", maxSeconds)
	}
	if scavengeTTL < 0 || int64(scavengeTTL) > maxSeconds {
		return fmt.Errorf("scavengettl must be between 0 and %d seconds", maxSeconds)
	}
	return nil
}
