// The MIT License (MIT)
//
// # Copyright (c) 2016 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
	"sync"

	"github.com/pkg/errors"
	"github.com/smithwhere/kcptun/std"
	kcp "github.com/xtaci/kcp-go/v5"
	"github.com/xtaci/tcpraw"
)

var (
	multiPort           *std.MultiPort
	multiPortParseError error
	multiPortOnce       sync.Once
	portMu              sync.Mutex
	previousPort        uint64
)

// dial establishes a connection to the configured remote endpoint.
func dial(config *Config, block kcp.BlockCrypt) (*kcp.UDPSession, error) {
	// Parse the multiPort definition only once.
	multiPortOnce.Do(func() {
		multiPort, multiPortParseError = std.ParseMultiPort(config.RemoteAddr)
	})

	// Abort when the multiPort definition is invalid.
	if multiPortParseError != nil {
		return nil, multiPortParseError
	}

	// Avoid picking the preceding destination again when the range has
	// multiple ports. Serialize selection for concurrent connection attempts.
	portMu.Lock()
	port, err := choosePort(multiPort.MinPort, multiPort.MaxPort, previousPort)
	if err == nil {
		previousPort = port
	}
	portMu.Unlock()
	if err != nil {
		return nil, err
	}
	remoteAddr := fmt.Sprintf("%v:%v", multiPort.Host, port)

	// Use tcpraw to emulate a TCP transport when requested.
	if config.TCP {
		conn, err := tcpraw.Dial("tcp", remoteAddr)
		if err != nil {
			return nil, errors.Wrap(err, "tcpraw.Dial()")
		}

		udpaddr, err := net.ResolveUDPAddr("udp", remoteAddr)
		if err != nil {
			conn.Close()
			return nil, errors.WithStack(err)
		}

		var convid uint32
		if err := binary.Read(rand.Reader, binary.LittleEndian, &convid); err != nil {
			conn.Close()
			return nil, errors.Wrap(err, "read convid")
		}

		kcpConn, err := kcp.NewConn4(convid, udpaddr, block, config.DataShard, config.ParityShard, true, conn)
		if err != nil {
			conn.Close()
			return nil, errors.Wrap(err, "kcp.NewConn4()")
		}
		return kcpConn, nil
	}

	// Otherwise fall back to the standard UDP dialing path.
	return kcp.DialWithOptions(remoteAddr, block, config.DataShard, config.ParityShard)
}

// choosePort samples uniformly from the range, excluding the previous port.
func choosePort(min, max, previous uint64) (uint64, error) {
	if min == 0 || max > 65535 || min > max {
		return 0, fmt.Errorf("invalid port range: %d-%d", min, max)
	}
	size := max - min + 1
	exclude := size > 1 && previous >= min && previous <= max
	if exclude {
		size--
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(size)))
	if err != nil {
		return 0, err
	}
	port := min + n.Uint64()
	if exclude && port >= previous {
		port++
	}
	return port, nil
}
