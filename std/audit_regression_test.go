package std

import (
	"encoding/json"
	"testing"
)

func TestRateLimitPortableJSON(t *testing.T) {
	var c BaseConfig
	if err := json.Unmarshal([]byte(`{"ratelimit":4294967295}`), &c); err != nil {
		t.Fatal(err)
	}
}
func TestSnmpOverflowDoesNotPanic(t *testing.T) {
	if ^uint(0)>>63 == 0 {
		t.Skip("64-bit overflow case")
	}
	value := int64(9223372037)
	SnmpLogger("unused.log", int(value))
}
