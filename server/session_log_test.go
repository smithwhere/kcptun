package main

import (
	"bytes"
	"log"
	"net"
	"strings"
	"testing"
)

// Distinct sessions must be attributable when the shared supervisor log reports
// a transport failure; a bare EOF or closed-pipe message loses that context.
func TestHandleMuxLogsSessionFailureContext(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	a, b := net.Pipe()
	defer a.Close()
	b.Close()
	config := &Config{
		Target: "127.0.0.1:1", SmuxVer: 1, SmuxBuf: 4 << 20,
		StreamBuf: 2 << 20, FrameSize: 8192, KeepAlive: 1,
	}
	handleMux(nil, a, config)
	for _, want := range []string{"smux session ended", "local=pipe", "remote=pipe", "duration=", "streams=0", "reason=EOF"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %q in session failure log: %s", want, output.String())
		}
	}
}
