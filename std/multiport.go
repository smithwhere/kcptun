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

package std

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type MultiPort struct {
	Host    string
	MinPort uint64
	MaxPort uint64
}

// ParseMultiPort parses a multiport listener or dialer address.
func ParseMultiPort(addr string) (*MultiPort, error) {
	separator := strings.LastIndexByte(addr, ':')
	if separator < 0 {
		return nil, errors.Errorf("malformed address:%v", addr)
	}

	host := addr[:separator]
	portSpec := addr[separator+1:]
	if portSpec == "" {
		return nil, errors.Errorf("malformed address:%v", addr)
	}

	// A colon in the host is valid only for bracketed IPv6 addresses. This
	// avoids treating an unbracketed IPv6 address as a host plus a port.
	if strings.Contains(host, ":") && !(strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]")) {
		return nil, errors.Errorf("malformed address:%v", addr)
	}

	parts := strings.Split(portSpec, "-")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" || (len(parts) == 2 && parts[1] == "") {
		return nil, errors.Errorf("malformed address:%v", addr)
	}

	parsePort := func(value string) (uint64, error) {
		port, err := strconv.ParseUint(value, 10, 16)
		if err != nil || port == 0 {
			return 0, errors.Errorf("invalid port range specified: %v", addr)
		}
		return port, nil
	}

	minPort, err := parsePort(parts[0])
	if err != nil {
		return nil, err
	}
	maxPort := minPort
	if len(parts) == 2 {
		maxPort, err = parsePort(parts[1])
		if err != nil {
			return nil, err
		}
	}

	if minPort > maxPort {
		return nil, errors.Errorf("invalid port range specified: minport:%v -> maxport %v", minPort, maxPort)
	}

	return &MultiPort{Host: host, MinPort: minPort, MaxPort: maxPort}, nil

}

