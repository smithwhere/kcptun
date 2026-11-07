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
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"strings"

	"github.com/pkg/errors"
	"github.com/smithwhere/kcptun/std"
	kcp "github.com/xtaci/kcp-go/v5"
	"github.com/xtaci/tcpraw"
)

// dial establishes a connection to the configured remote endpoint.
func dial(config *Config, block kcp.BlockCrypt) (*kcp.UDPSession, error) {
	return dialContext(context.Background(), config, block)
}

func dialContext(ctx context.Context, config *Config, block kcp.BlockCrypt) (*kcp.UDPSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The address belongs to this configuration, not to the process. Caching
	// it globally would silently route later calls to an earlier remote.
	multiPort, err := std.ParseMultiPort(config.RemoteAddr)
	if err != nil {
		return nil, err
	}

	// Pick a random destination port within the configured range.
	var randport uint64
	err = binary.Read(rand.Reader, binary.LittleEndian, &randport)
	if err != nil {
		return nil, err
	}

	port := uint64(multiPort.MinPort) + randport%uint64(multiPort.MaxPort-multiPort.MinPort+1)
	host := strings.TrimSuffix(strings.TrimPrefix(multiPort.Host, "["), "]")
	remoteAddr, err := resolveRemote(ctx, net.DefaultResolver, host, port)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Use tcpraw to emulate a TCP transport when requested.
	if config.TCP {
		conn, err := tcpraw.DialContext(ctx, "tcp", remoteAddr)
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

func resolveRemote(ctx context.Context, resolver *net.Resolver, host string, port uint64) (string, error) {
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no address for %s", host)
	}
	selected := preferredIP(ips)
	return net.JoinHostPort(selected.String(), fmt.Sprint(port)), nil
}

// Match Go ResolveUDPAddr/ResolveTCPAddr: prefer IPv4 for hostnames.
func preferredIP(ips []net.IPAddr) net.IPAddr {
	for _, ip := range ips {
		if ip.IP.To4() != nil {
			return ip
		}
	}
	return ips[0]
}
