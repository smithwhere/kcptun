// The MIT License (MIT)
//
// Copyright (c) 2019 xtaci
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

//go:build linux

package tcpraw

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coreos/go-iptables/iptables"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

var (
	errOpNotImplemented = errors.New("operation not implemented") // Error for unimplemented operations
	errTimeout          = errors.New("timeout")                   // Error for operation timeout
	expire              = time.Minute                             // Duration to define expiration time for flows
)

var (
	connList   list.List
	connListMu sync.Mutex
)

// a message from NIC
type message struct {
	bts  []byte
	addr net.Addr
}

// a tcp flow information of a connection pair
type tcpFlow struct {
	conn         *net.TCPConn               // the related system TCP connection of this flow
	handle       *net.IPConn                // the handle to send packets
	seq          uint32                     // TCP sequence number
	ack          uint32                     // TCP acknowledge number
	tsEcr        uint32                     // TCP timestamp echo reply
	networkLayer gopacket.SerializableLayer // network layer header for tx
	ts           time.Time                  // last packet incoming time
	buf          gopacket.SerializeBuffer   // a buffer for write
	tcpHeader    layers.TCP
}

// TCPConn
type TCPConn struct {
	// a wrapper for tcpconn for gc purpose
	*tcpConn
}

// tcpConn defines a TCP-packet oriented connection
type tcpConn struct {
	elem    *list.Element // elem in the list
	die     chan struct{}
	dieOnce sync.Once

	// the main golang sockets
	tcpconn  *net.TCPConn     // from net.Dial
	listener *net.TCPListener // from net.Listen

	// handles
	handles []*net.IPConn

	// packets captured from all related NICs will be delivered to this channel
	chMessage chan message

	// all TCP flows
	flowTable map[string]*tcpFlow
	flowsLock sync.Mutex

	// iptables
	iptables  *iptables.IPTables // Handle for IPv4 iptables rules
	iprule    []string           // IPv4 iptables rule associated with the connection
	ip6tables *iptables.IPTables // Handle for IPv6 iptables rules
	ip6rule   []string           // IPv6 iptables rule associated with the connection

	// deadlines
	readDeadline  atomic.Value // Atomic value for read deadline
	writeDeadline atomic.Value // Atomic value for write deadline

	// serialization
	opts gopacket.SerializeOptions

	// fingerprints
	tcpFingerPrint fingerPrint
}

// lockflow locks the flow table and apply function `f` to the entry, and create one if not exist
func (conn *tcpConn) lockflow(addr net.Addr, f func(e *tcpFlow)) {
	key := addr.String()  // Use the string representation of the address as the key
	conn.flowsLock.Lock() // Lock the flowTable for safe access
	e := conn.flowTable[key]
	if e == nil { // entry first visit
		e = new(tcpFlow)                      // Create a new flow if it doesn't exist
		e.ts = time.Now()                     // Set the timestamp to the current time
		e.buf = gopacket.NewSerializeBuffer() // Initialize the serialization buffer
	}
	f(e)                    // Apply the function to the flow entry
	conn.flowTable[key] = e // Store the modified flow entry back into the table
	conn.flowsLock.Unlock() // Unlock the flowTable
}

// clean expired flows
func (conn *tcpConn) cleaner() {
	ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-conn.die: // Exit if the connection is closed
			return
		case <-ticker.C: // On each tick, clean up expired flows
			conn.flowsLock.Lock()
			now := time.Now()
			for k, v := range conn.flowTable {
				ttl := expire
				if v.conn == nil {
					ttl = 5 * time.Second // Short expire for orphans
				}

				if now.Sub(v.ts) > ttl { // Check if the flow has expired
					if v.conn != nil {
						setTTL(v.conn, 64) // Set TTL before closing the connection
						v.conn.Close()
					}
					delete(conn.flowTable, k) // Remove the flow from the table
				}
			}
			conn.flowsLock.Unlock()
		}
	}
}

// captureFlow capture every inbound packets based on rules of BPF
func (conn *tcpConn) captureFlow(handle *net.IPConn, port int) {
	buf := make([]byte, 2048)
	opt := gopacket.DecodeOptions{NoCopy: true, Lazy: true}
	for {
		n, addr, err := handle.ReadFromIP(buf)
		if err != nil {
			return
		}

		// try decoding TCP frame from buf[:n]
		packet := gopacket.NewPacket(buf[:n], layers.LayerTypeTCP, opt)
		transport := packet.TransportLayer()
		tcp, ok := transport.(*layers.TCP)
		if !ok {
			continue
		}

		// port filtering
		if int(tcp.DstPort) != port {
			continue
		}

		// Preserve the interface scope when matching link-local flows.
		zone := handle.LocalAddr().(*net.IPAddr).Zone
		if conn.tcpconn != nil {
			if remote, ok := conn.tcpconn.RemoteAddr().(*net.TCPAddr); ok && remote.Zone != "" {
				zone = remote.Zone
			}
		}
		src := flowSourceAddress(addr, int(tcp.SrcPort), zone)

		var orphan bool
		// flow maintaince
		conn.lockflow(src, func(e *tcpFlow) {
			if e.conn == nil { // make sure it's related to net.TCPConn
				orphan = true // mark as orphan if it's not related net.TCPConn
			}

			// to keep track of TCP header related to this source
			e.ts = time.Now()
			if tcp.ACK {
				e.seq = tcp.Ack
			}

			// Parse TCP options to get Timestamp
			if ts, ok := timestampValue(tcp.Options); ok {
				e.tsEcr = ts
			}

			// Update ACK
			nextSeq := tcp.Seq + uint32(len(tcp.Payload))
			if tcp.SYN {
				nextSeq++
			}
			if tcp.FIN {
				nextSeq++
			}

			// If we have payload or flags that consume sequence space, update ack
			if nextSeq != tcp.Seq {
				if e.ack == 0 || e.ack == tcp.Seq {
					e.ack = nextSeq
				}
			}

			e.handle = handle
		})

		// push data if it's not orphan
		if !orphan && tcp.PSH {
			payload := make([]byte, len(tcp.Payload))
			copy(payload, tcp.Payload)
			select {
			case conn.chMessage <- message{payload, src}:
			case <-conn.die:
				return
			}
		}
	}
}

// ReadFrom implements the PacketConn ReadFrom method.
func (conn *tcpConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	var timer *time.Timer
	var deadline <-chan time.Time
	if d, ok := conn.readDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer = time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline = timer.C
	}

	select {
	case <-deadline:
		return 0, nil, errTimeout
	case <-conn.die:
		return 0, nil, io.EOF
	case packet := <-conn.chMessage:
		n = copy(p, packet.bts)
		return n, packet.addr, nil
	}
}

// WriteTo implements the PacketConn WriteTo method.
func (conn *tcpConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	var deadline <-chan time.Time
	if d, ok := conn.writeDeadline.Load().(time.Time); ok && !d.IsZero() {
		timer := time.NewTimer(time.Until(d))
		defer timer.Stop()
		deadline = timer.C
	}

	select {
	case <-deadline:
		return 0, errTimeout
	case <-conn.die:
		return 0, io.EOF
	default:
		var raddr *net.TCPAddr
		raddr, err = net.ResolveTCPAddr("tcp", addr.String())
		if err != nil {
			return 0, err
		}

		var lport int
		if conn.tcpconn != nil {
			lport = conn.tcpconn.LocalAddr().(*net.TCPAddr).Port
		} else {
			lport = conn.listener.Addr().(*net.TCPAddr).Port
		}

		conn.lockflow(addr, func(e *tcpFlow) {
			// if the flow doesn't have handle , assume this packet has lost, without notification
			if e.handle == nil {
				n = len(p)
				return
			}

			// build tcp header with local and remote port
			e.tcpHeader.SrcPort = layers.TCPPort(lport)
			e.tcpHeader.DstPort = layers.TCPPort(raddr.Port)
			e.tcpHeader.Window = conn.tcpFingerPrint.Window
			e.tcpHeader.Ack = e.ack
			e.tcpHeader.Seq = e.seq
			e.tcpHeader.PSH = true
			e.tcpHeader.ACK = true
			e.tcpHeader.Options = conn.tcpFingerPrint.Options
			makeOption(conn.tcpFingerPrint.Type, e.tcpHeader.Options, e.tsEcr)

			// build IP header with src & dst ip for TCP checksum
			if raddr.IP.To4() != nil {
				ip := &layers.IPv4{
					Protocol: layers.IPProtocolTCP,
					SrcIP:    e.handle.LocalAddr().(*net.IPAddr).IP.To4(),
					DstIP:    raddr.IP.To4(),
				}
				e.tcpHeader.SetNetworkLayerForChecksum(ip)
			} else {
				ip := &layers.IPv6{
					NextHeader: layers.IPProtocolTCP,
					SrcIP:      e.handle.LocalAddr().(*net.IPAddr).IP.To16(),
					DstIP:      raddr.IP.To16(),
				}
				e.tcpHeader.SetNetworkLayerForChecksum(ip)
			}

			e.buf.Clear()
			if err = gopacket.SerializeLayers(e.buf, conn.opts, &e.tcpHeader, gopacket.Payload(p)); err != nil {
				return
			}
			if conn.tcpconn != nil {
				n, err = writeRawFrame(len(p), e.buf.Bytes(), e.handle.Write)
			} else {
				n, err = writeRawFrame(len(p), e.buf.Bytes(), func(frame []byte) (int, error) {
					return e.handle.WriteToIP(frame, &net.IPAddr{IP: raddr.IP, Zone: raddr.Zone})
				})
			}
			if err == nil {
				e.seq += uint32(len(p))
			}
		})
	}
	return
}

// Close closes the connection.
func (conn *tcpConn) Close() error {
	var err error

	conn.dieOnce.Do(func() {
		// signal closing
		close(conn.die)

		// close all established tcp connections
		if conn.tcpconn != nil { // client
			setTTL(conn.tcpconn, 64)
			err = conn.tcpconn.Close()
		} else if conn.listener != nil {
			err = conn.listener.Close() // server
			conn.flowsLock.Lock()
			for k, v := range conn.flowTable {
				if v.conn != nil {
					setTTL(v.conn, 64)
					v.conn.Close()
				}
				delete(conn.flowTable, k)
			}
			conn.flowsLock.Unlock()
		}

		// close handles
		for k := range conn.handles {
			conn.handles[k].Close()
		}

		// Report cleanup failures to callers and logs; rules must not fail silently.
		if conn.iptables != nil {
			if e := deleteFirewallRule(conn.iptables, conn.iprule); e != nil {
				log.Printf("tcpraw: %v", e)
				err = errors.Join(err, e)
			}
		}
		if conn.ip6tables != nil {
			if e := deleteFirewallRule(conn.ip6tables, conn.ip6rule); e != nil {
				log.Printf("tcpraw: %v", e)
				err = errors.Join(err, e)
			}
		}
		// remove from the global list
		connListMu.Lock()
		if conn.elem != nil {
			connList.Remove(conn.elem)
			conn.elem = nil
		}
		connListMu.Unlock()
	})
	return err
}

func (conn *tcpConn) setupFirewall(ipv6 bool, rule []string) error {
	return conn.setupFirewallContext(context.Background(), ipv6, rule)
}

func (conn *tcpConn) setupFirewallContext(ctx context.Context, ipv6 bool, rule []string) error {
	proto := iptables.ProtocolIPv4
	if ipv6 {
		proto = iptables.ProtocolIPv6
	}
	ipt, err := iptables.NewContext(ctx, iptables.IPFamily(proto))
	if err != nil {
		return fmt.Errorf("initialize firewall for protocol %v: %w", proto, err)
	}
	owned, err := installFirewallRuleContext(ctx, ipt, rule)
	if owned {
		if ipv6 {
			conn.ip6tables, conn.ip6rule = ipt, rule
		} else {
			conn.iptables, conn.iprule = ipt, rule
		}
	}
	return err
}

// LocalAddr returns the local network address.
func (conn *tcpConn) LocalAddr() net.Addr {
	if conn.tcpconn != nil {
		return conn.tcpconn.LocalAddr()
	} else if conn.listener != nil {
		return conn.listener.Addr()
	}
	return nil
}

// SetDeadline implements the Conn SetDeadline method.
func (conn *tcpConn) SetDeadline(t time.Time) error {
	if err := conn.SetReadDeadline(t); err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// SetReadDeadline implements the Conn SetReadDeadline method.
func (conn *tcpConn) SetReadDeadline(t time.Time) error {
	conn.readDeadline.Store(t)
	return nil
}

// SetWriteDeadline implements the Conn SetWriteDeadline method.
func (conn *tcpConn) SetWriteDeadline(t time.Time) error {
	conn.writeDeadline.Store(t)
	return nil
}

// SetDSCP sets the 6bit DSCP field in IPv4 header, or 8bit Traffic Class in IPv6 header.
func (conn *tcpConn) SetDSCP(dscp int) error {
	for k := range conn.handles {
		if err := setDSCP(conn.handles[k], dscp); err != nil {
			return err
		}
	}
	return nil
}

// SetReadBuffer sets the size of the operating system's receive buffer associated with the connection.
func (conn *tcpConn) SetReadBuffer(bytes int) error {
	var err error
	for k := range conn.handles {
		if err := conn.handles[k].SetReadBuffer(bytes); err != nil {
			return err
		}
	}
	return err
}

// SetWriteBuffer sets the size of the operating system's transmit buffer associated with the connection.
func (conn *tcpConn) SetWriteBuffer(bytes int) error {
	var err error
	for k := range conn.handles {
		if err := conn.handles[k].SetWriteBuffer(bytes); err != nil {
			return err
		}
	}
	return err
}

// Dial connects to the remote TCP port,
// and returns a single packet-oriented connection
func Dial(network, address string) (*TCPConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return DialContext(ctx, network, address)
}

func DialContext(ctx context.Context, network, address string) (*TCPConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// remote address resolve
	raddr, err := resolveTCPAddrContext(ctx, net.DefaultResolver, network, address)
	if err != nil {
		return nil, err
	}

	// AF_INET
	rawHandle, err := (&net.Dialer{}).DialContext(ctx, "ip:tcp", rawDialAddress(raddr))
	if err != nil {
		return nil, err
	}

	handle, ok := rawHandle.(*net.IPConn)
	if !ok {
		rawHandle.Close()
		return nil, fmt.Errorf("unexpected connection type %T", rawHandle)
	}

	// create an established tcp connection
	// will hack this tcp connection for packet transmission
	rawTCP, err := (&net.Dialer{}).DialContext(ctx, network, raddr.String())
	if err != nil {
		handle.Close()
		return nil, err
	}

	tcpconn, ok := rawTCP.(*net.TCPConn)
	if !ok {
		rawTCP.Close()
		handle.Close()
		return nil, fmt.Errorf("unexpected connection type %T", rawTCP)
	}

	// parse local ip and port from tcpconn
	_, lport, err := net.SplitHostPort(tcpconn.LocalAddr().String())
	if err != nil {
		tcpconn.Close()
		handle.Close()
		return nil, err
	}

	// fields
	conn := new(tcpConn)
	conn.die = make(chan struct{})
	conn.flowTable = make(map[string]*tcpFlow)
	conn.tcpconn = tcpconn
	conn.chMessage = make(chan message)
	conn.lockflow(tcpconn.RemoteAddr(), func(e *tcpFlow) { e.conn = tcpconn })
	conn.handles = append(conn.handles, handle)
	conn.opts = gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}
	conn.tcpFingerPrint = fingerPrintLinux.Clone()

	go conn.captureFlow(handle, tcpconn.LocalAddr().(*net.TCPAddr).Port)
	go conn.cleaner()

	// iptables
	err = setTTL(tcpconn, 1)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Only install the rule for the address family actually used.
	ipv6 := raddr.IP.To4() == nil
	match := []string{"-m", "ttl", "--ttl-eq", "1"}
	if ipv6 {
		match = []string{"-m", "hl", "--hl-eq", "1"}
	}
	rule := append(match, "-p", "tcp", "-s", tcpconn.LocalAddr().(*net.TCPAddr).IP.String(), "--sport", lport, "-d", raddr.IP.String(), "--dport", fmt.Sprint(raddr.Port), "-j", "DROP")
	if err := conn.setupFirewallContext(ctx, ipv6, rule); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, conn.Close())
	}

	// discard everything
	go io.Copy(ioutil.Discard, tcpconn)

	// push back to the global list and set the elem
	connListMu.Lock()
	conn.elem = connList.PushBack(conn)
	connListMu.Unlock()

	return wrapConn(conn), nil
}

// Listen acts like net.ListenTCP,
// and returns a single packet-oriented connection
func Listen(network, address string) (*TCPConn, error) {
	// fields
	conn := new(tcpConn)
	conn.flowTable = make(map[string]*tcpFlow)
	conn.die = make(chan struct{})
	conn.chMessage = make(chan message)
	conn.opts = gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}
	conn.tcpFingerPrint = fingerPrintLinux.Clone()

	// resolve address
	laddr, err := net.ResolveTCPAddr(network, address)
	if err != nil {
		return nil, err
	}

	// AF_INET
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	if laddr.IP == nil || laddr.IP.IsUnspecified() { // if address is not specified, capture on all ifaces
		var lasterr error
		for _, iface := range ifaces {
			if addrs, err := iface.Addrs(); err == nil {
				for _, addr := range addrs {
					if ipaddr, ok := addr.(*net.IPNet); ok {
						zone := ""
						if ipaddr.IP.IsLinkLocalUnicast() && ipaddr.IP.To4() == nil {
							zone = iface.Name
						}
						if handle, err := net.ListenIP("ip:tcp", &net.IPAddr{IP: ipaddr.IP, Zone: zone}); err == nil {
							conn.handles = append(conn.handles, handle)
							go conn.captureFlow(handle, laddr.Port)
						} else {
							lasterr = err
						}
					}
				}
			}
		}
		if len(conn.handles) == 0 {
			return nil, lasterr
		}
	} else {
		if handle, err := net.ListenIP("ip:tcp", rawListenAddress(laddr)); err == nil {
			conn.handles = append(conn.handles, handle)
			go conn.captureFlow(handle, laddr.Port)
		} else {
			return nil, err
		}
	}

	// start listening
	l, err := net.ListenTCP(network, laddr)
	if err != nil {
		for _, h := range conn.handles {
			h.Close()
		}
		return nil, err
	}

	conn.listener = l

	// start cleaner
	go conn.cleaner()

	// Install rules for the address families covered by the listener.
	need4, need6 := false, false
	if laddr.IP != nil && !laddr.IP.IsUnspecified() {
		need4 = laddr.IP.To4() != nil
		need6 = !need4
	} else {
		for _, h := range conn.handles {
			ip := h.LocalAddr().(*net.IPAddr).IP
			if ip.To4() != nil && network != "tcp6" {
				need4 = true
			}
			if ip.To4() == nil && network != "tcp4" {
				need6 = true
			}
		}
	}
	port := l.Addr().(*net.TCPAddr).Port
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	owner := "kcptun-" + hex.EncodeToString(token[:])
	if need4 {
		rule := listenerFirewallRule(false, laddr.IP, port, owner)
		if err := conn.setupFirewall(false, rule); err != nil {
			return nil, errors.Join(err, conn.Close())
		}
	}
	if need6 {
		rule := listenerFirewallRule(true, laddr.IP, port, owner)
		if err := conn.setupFirewall(true, rule); err != nil {
			return nil, errors.Join(err, conn.Close())
		}
	}
	// discard everything in original connection
	go func() {
		for {
			tcpconn, err := l.AcceptTCP()
			if err != nil {
				return
			}

			if err := prepareAcceptedSocket(tcpconn, func() error { return setTTL(tcpconn, 1) }); err != nil {
				log.Printf("tcpraw: configure accepted connection: %v", err)
				continue
			}

			// record net.Conn
			conn.lockflow(tcpconn.RemoteAddr(), func(e *tcpFlow) { e.conn = tcpconn })

			// discard everything
			go io.Copy(ioutil.Discard, tcpconn)
		}
	}()

	// push back to the global list and set the elem
	connListMu.Lock()
	conn.elem = connList.PushBack(conn)
	connListMu.Unlock()

	return wrapConn(conn), nil
}

// setTTL sets the Time-To-Live field on a given connection
func setTTL(c *net.TCPConn, ttl int) error {
	raw, err := c.SyscallConn()
	if err != nil {
		return err
	}
	addr := c.LocalAddr().(*net.TCPAddr)
	return controlSocket(raw, func(fd uintptr) error {
		if addr.IP.To4() == nil {
			return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_UNICAST_HOPS, ttl)
		}
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
	})
}

// setDSCP sets the 6bit DSCP field in IPv4 header, or 8bit Traffic Class in IPv6 header.
func setDSCP(c *net.IPConn, dscp int) error {
	raw, err := c.SyscallConn()
	if err != nil {
		return err
	}
	addr := c.LocalAddr().(*net.IPAddr)
	return controlSocket(raw, func(fd uintptr) error {
		if addr.IP.To4() == nil {
			return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, syscall.IPV6_TCLASS, dscp)
		}
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, dscp<<2)
	})
}

// wrapConn wraps a tcpConn in a TCPConn.
func wrapConn(conn *tcpConn) *TCPConn {
	// Set up a finalizer to ensure resources are cleaned up when the TCPConn is garbage collected
	wrapper := &TCPConn{tcpConn: conn}
	runtime.SetFinalizer(wrapper, func(wrapper *TCPConn) {
		wrapper.Close()
	})

	return wrapper
}
