package std

import (
	"testing"

	"github.com/xtaci/kcp-go/v5"
)

func TestValidateFECParams(t *testing.T) {
	for _, tt := range []struct {
		data, parity int
		invalid      bool
	}{
		{10, 3, false}, {255, 1, false}, {1, 255, false}, {0, 0, false}, {10, 0, false}, {0, 3, false},
		{-1, 3, true}, {10, -1, true}, {256, 1, true}, {1, 256, true}, {257, 257, true},
		{int(^uint(0) >> 1), int(^uint(0) >> 1), true},
	} {
		if err := ValidateFECParams(tt.data, tt.parity); (err != nil) != tt.invalid {
			t.Errorf("FEC(%d,%d): %v", tt.data, tt.parity, err)
		}
	}
}

func TestValidateMTUBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name, crypt           string
		data, parity, minimum int
	}{
		{"plain", "null", 0, 0, 50},
		{"aes", "aes", 0, 0, 70},
		{"aesFEC", "aes", 10, 3, 78},
		{"noCryptFEC", "null", 10, 3, 58},
		{"gcm", "aes-128-gcm", 0, 0, 78},
		{"gcmFEC", "aes-128-gcm", 10, 3, 86},
	} {
		t.Run(tt.name, func(t *testing.T) {
			block, _ := SelectBlockCrypt(tt.crypt, make([]byte, 32))
			for _, mtu := range []int{-1, 0, tt.minimum - 1, tt.minimum, 1350, 1500, 1501} {
				invalid := mtu < tt.minimum || mtu > 1500
				if err := ValidateMTU(mtu, block, tt.data, tt.parity); (err != nil) != invalid {
					t.Errorf("MTU %d: %v", mtu, err)
				}
			}
			// Verify the boundary against the actual KCP implementation too.
			conn, err := kcp.DialWithOptions("127.0.0.1:29900", block, tt.data, tt.parity)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if conn.SetMtu(tt.minimum - 1) {
				t.Fatal("KCP accepted below minimum")
			}
			if !conn.SetMtu(tt.minimum) {
				t.Fatal("KCP rejected minimum")
			}
		})
	}
}

func TestValidateRateLimit(t *testing.T) {
	for _, tt := range []struct {
		rate int
		bad  bool
	}{
		{0, false},
		{1, false},
		{1000000, false},
		{-1, true},
	} {
		if err := ValidateRateLimit(tt.rate); (err != nil) != tt.bad {
			t.Errorf("ValidateRateLimit(%d) = %v, want bad=%v", tt.rate, err, tt.bad)
		}
	}
	maxRate := uint64(^uint32(0))
	if uint64(^uint(0)>>1) > maxRate {
		for _, value := range []uint64{maxRate, maxRate + 1} {
			rate := int(value)
			invalid := value > maxRate
			if err := ValidateRateLimit(rate); (err != nil) != invalid {
				t.Errorf("ValidateRateLimit(%d) = %v, want invalid=%v", rate, err, invalid)
			}
		}
	}
}
