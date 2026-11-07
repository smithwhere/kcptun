package tcpraw

import (
	"encoding/binary"
	"testing"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

func TestLinuxFingerprintTimestampWireFormat(t *testing.T) {
	fp := fingerPrintLinux.Clone()
	makeOption(TypeLinux, fp.Options, 0x12345678)
	tcp := &layers.TCP{Options: fp.Options}
	buf := gopacket.NewSerializeBuffer()
	if err := tcp.SerializeTo(buf, gopacket.SerializeOptions{FixLengths: true}); err != nil {
		t.Fatal(err)
	}
	var decoded layers.TCP
	if err := decoded.DecodeFromBytes(buf.Bytes(), gopacket.NilDecodeFeedback); err != nil {
		t.Fatal(err)
	}
	for _, option := range decoded.Options {
		if option.OptionType == 8 {
			if option.OptionLength != 10 || len(option.OptionData) != 8 {
				t.Fatalf("timestamp length=%d data=%d, want 10 and 8", option.OptionLength, len(option.OptionData))
			}
			if got := binary.BigEndian.Uint32(option.OptionData[4:]); got != 0x12345678 {
				t.Fatalf("echo timestamp = %x", got)
			}
			return
		}
	}
	t.Fatal("timestamp option missing")
}

func TestTimestampValueFromWireOptions(t *testing.T) {
	options := []layers.TCPOption{{OptionType: layers.TCPOptionKindTimestamps, OptionLength: 10, OptionData: []byte{0x12, 0x34, 0x56, 0x78, 0, 0, 0, 0}}}
	got, ok := timestampValue(options)
	if !ok || got != 0x12345678 {
		t.Fatalf("timestamp=%x, found=%v", got, ok)
	}
	options[0].OptionData = options[0].OptionData[:4]
	if _, ok := timestampValue(options); ok {
		t.Fatal("accepted truncated timestamp")
	}
}
