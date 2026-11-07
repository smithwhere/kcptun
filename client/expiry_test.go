package main

import (
	"strconv"
	"testing"
)

func TestValidateExpiry(t *testing.T) {
	for _, c := range []struct {
		a, b  int
		valid bool
	}{{0, 0, true}, {10, 0, true}, {10, 5, true}, {-1, 0, false}, {10, -1, false}} {
		err := validateExpiry(c.a, c.b)
		if (err == nil) != c.valid {
			t.Errorf("validateExpiry(%d,%d) = %v", c.a, c.b, err)
		}
	}
	if strconv.IntSize == 64 {
		n, _ := strconv.ParseInt("9223372037", 10, 64)
		if validateExpiry(int(n), 0) == nil || validateExpiry(0, int(n)) == nil {
			t.Error("overflow accepted")
		}
	}
}
