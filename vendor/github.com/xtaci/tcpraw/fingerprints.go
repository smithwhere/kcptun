package tcpraw

import (
	"encoding/binary"
	"time"

	"github.com/google/gopacket/layers"
)

type FingerPrintType int

const (
	TypeLinux FingerPrintType = iota
)

type fingerPrint struct {
	Type    FingerPrintType
	Window  uint16
	Options []layers.TCPOption
	TTL     uint16
}

func (f fingerPrint) Clone() fingerPrint {
	c := f
	c.Options = make([]layers.TCPOption, len(f.Options))
	for i, opt := range f.Options {
		c.Options[i] = opt
		if opt.OptionData != nil {
			c.Options[i].OptionData = make([]byte, len(opt.OptionData))
			copy(c.Options[i].OptionData, opt.OptionData)
		}
	}
	return c
}

// options [nop,nop,TS val 1940162183 ecr 1366690553]
var fingerPrintLinux = fingerPrint{
	Type:   TypeLinux,
	Window: 65535,
	Options: []layers.TCPOption{
		{OptionType: 1},
		{OptionType: 1},
		{OptionType: 8, OptionLength: 10, OptionData: make([]byte, 8)},
	},
	TTL: 64,
}

var defaultFingerPrint = fingerPrintLinux

func init() {
}

func timestampValue(options []layers.TCPOption) (uint32, bool) {
	for _, opt := range options {
		if opt.OptionType == layers.TCPOptionKindTimestamps && len(opt.OptionData) == 8 {
			return binary.BigEndian.Uint32(opt.OptionData[:4]), true
		}
	}
	return 0, false
}

func makeOption(optType FingerPrintType, options []layers.TCPOption, tsecr uint32) {
	switch optType {
	case TypeLinux:
		// Timestamps: Kind 8, Length 10
		for i := range options {
			if options[i].OptionType == 8 && len(options[i].OptionData) == 8 {
				// Use milliseconds for timestamp to be more realistic (RFC 7323)
				nowMillis := time.Now().UnixNano() / 1e6
				binary.BigEndian.PutUint32(options[i].OptionData[:4], uint32(nowMillis))
				binary.BigEndian.PutUint32(options[i].OptionData[4:], tsecr)
				break
			}
		}
	}
}
