//go:build linux

package iptables

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestContextCancelsVersionAndRuleCommands(t *testing.T) {
	dir := t.TempDir()
	versionBlock := filepath.Join(dir, "version-block")
	if err := os.WriteFile(versionBlock, []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	start := time.Now()
	_, err := NewContext(ctx, Path(versionBlock))
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("version error: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("version cancellation took %v", time.Since(start))
	}
	ruleBlock := filepath.Join(dir, "rule-block")
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'iptables v1.8.9'; exit 0; fi\nexec sleep 10\n"
	if err := os.WriteFile(ruleBlock, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	ipt, err := NewContext(context.Background(), Path(ruleBlock))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	start = time.Now()
	_, err = ipt.ExistsContext(ctx, "filter", "OUTPUT", "-j", "DROP")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rule error: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("rule cancellation took %v", time.Since(start))
	}
}
