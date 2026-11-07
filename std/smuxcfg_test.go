package std

import (
	"fmt"
	"github.com/xtaci/smux"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestBuildSmuxConfigKeepAliveBounds(t *testing.T) {
	for _, tt := range []struct {
		seconds int
		wantErr bool
	}{
		{-1, true},
		{0, true},
		{1, false},
		{10, false},
		{30, false},
		{31, false},
	} {
		t.Run(fmt.Sprint(tt.seconds), func(t *testing.T) {
			_, err := BuildSmuxConfig(1, 4194304, 2097152, 8192, tt.seconds)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildSmuxConfig keepalive=%d error = %v, wantErr %t", tt.seconds, err, tt.wantErr)
			}
		})
	}
}

func TestKeepAliveHasTimeoutMargin(t *testing.T) {
	for _, seconds := range []int{1, 10, 30, 31, 300} {
		cfg, err := BuildSmuxConfig(1, 4194304, 2097152, 8192, seconds)
		if err != nil {
			t.Fatalf("keepalive %d: %v", seconds, err)
		}
		if cfg.KeepAliveTimeout < 3*cfg.KeepAliveInterval {
			t.Fatalf("keepalive %d: timeout=%v interval=%v", seconds, cfg.KeepAliveTimeout, cfg.KeepAliveInterval)
		}
	}
}

func TestIdleSmuxSessionsRemainConnectedWithHeartbeatMargin(t *testing.T) {
	cfg, err := BuildSmuxConfig(1, 4194304, 2097152, 8192, 30)
	if err != nil {
		t.Fatal(err)
	}
	// Scale both values down to exercise several cycles without a long test.
	cfg.KeepAliveInterval /= 1000
	cfg.KeepAliveTimeout /= 1000
	a, b := net.Pipe()
	client, err := smux.Client(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := smux.Server(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	select {
	case <-client.CloseChan():
		t.Fatal("healthy idle client closed")
	case <-server.CloseChan():
		t.Fatal("healthy idle server closed")
	case <-time.After(250 * time.Millisecond):
	}
}

func TestKeepAliveTimeoutDoesNotOverflow(t *testing.T) {
	max := int64((1<<63 - 1) / (3 * time.Second))
	if strconv.IntSize == 64 {
		cfg, err := BuildSmuxConfig(1, 4194304, 2097152, 8192, int(max))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.KeepAliveTimeout < cfg.KeepAliveInterval {
			t.Fatal("timeout overflow")
		}
		if _, err := BuildSmuxConfig(1, 4194304, 2097152, 8192, int(max+1)); err == nil {
			t.Fatal("overflowing interval accepted")
		}
	}
}
