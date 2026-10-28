package std

import (
	"fmt"

	kcp "github.com/xtaci/kcp-go/v5"
)

// ValidateFECParams checks the limits shared by the KCP encoder and decoder.
// Either zero shard count disables FEC, as supported by kcp-go.
func ValidateFECParams(data, parity int) error {
	if data < 0 || parity < 0 {
		return fmt.Errorf("FEC shard counts must not be negative")
	}
	if data == 0 || parity == 0 {
		return nil
	}
	// Check each operand before adding to avoid integer overflow.
	if data > 256 || parity > 256 || data > 256-parity {
		return fmt.Errorf("FEC data and parity shard counts must total at most 256")
	}
	return nil
}

// ValidateRateLimit protects the uint32 conversion passed to kcp-go.
// Values exceeding uint32 would wrap and silently disable/alter pacing.
func ValidateRateLimit(rate int) error {
	const maxRate = uint64(^uint32(0))
	if rate < 0 || uint64(rate) > maxRate {
		return fmt.Errorf("ratelimit must be between 0 and %d", maxRate)
	}
	return nil
}

// ValidateMTU accounts for the same wire overhead as UDPSession.SetMtu.
func ValidateMTU(mtu int, block kcp.BlockCrypt, data, parity int) error {
	// kcp-go requires an inner MTU of at least 50 and caps packets at 1500.
	minimum := 50
	if block != nil {
		if aead, ok := block.(interface {
			NonceSize() int
			Overhead() int
		}); ok {
			minimum += aead.NonceSize() + aead.Overhead()
		} else {
			minimum += 20 // nonce (16) plus checksum (4)
		}
	}
	if data > 0 && parity > 0 {
		minimum += 8
	} // FEC header and size field
	if mtu < minimum || mtu > 1500 {
		return fmt.Errorf("MTU must be between %d and 1500 for this encryption/FEC configuration", minimum)
	}
	return nil
}
