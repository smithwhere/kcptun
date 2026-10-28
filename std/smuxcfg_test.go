package std

import (
	"fmt"
	"testing"
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
		{31, true},
	} {
		t.Run(fmt.Sprint(tt.seconds), func(t *testing.T) {
			_, err := BuildSmuxConfig(1, 4194304, 2097152, 8192, tt.seconds)
			if (err != nil) != tt.wantErr {
				t.Fatalf("BuildSmuxConfig keepalive=%d error = %v, wantErr %t", tt.seconds, err, tt.wantErr)
			}
		})
	}
}
