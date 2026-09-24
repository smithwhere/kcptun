package main

import "testing"

func TestSmuxVersionFlagDefaultsToV1(t *testing.T) {
	flag := smuxVersionFlag()
	if flag.Value != 1 {
		t.Fatalf("smuxver default = %d, want 1", flag.Value)
	}
}
