package tcpraw

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
)

func writeRawFrame(payloadLen int, frame []byte, write func([]byte) (int, error)) (int, error) {
	n, err := write(frame)
	if err != nil {
		return 0, err
	}
	if n != len(frame) {
		return 0, io.ErrShortWrite
	}
	return payloadLen, nil
}

// resolveTCPAddrContext avoids an uncancellable hostname lookup in DialContext.
func resolveTCPAddrContext(ctx context.Context, resolver *net.Resolver, network, address string) (*net.TCPAddr, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, net.UnknownNetworkError(network)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	portNumber, err := resolver.LookupPort(ctx, "tcp", port)
	if err != nil {
		return nil, err
	}
	if host == "" || net.ParseIP(strings.SplitN(host, "%", 2)[0]) != nil {
		return net.ResolveTCPAddr(network, net.JoinHostPort(host, strconv.Itoa(portNumber)))
	}
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	// ResolveTCPAddr prefers IPv4 for the generic tcp network.
	for _, ipv4 := range []bool{true, false} {
		if (network == "tcp4" && !ipv4) || (network == "tcp6" && ipv4) {
			continue
		}
		for _, ip := range ips {
			if (ip.IP.To4() != nil) == ipv4 {
				return net.ResolveTCPAddr(network, net.JoinHostPort(ip.String(), strconv.Itoa(portNumber)))
			}
		}
	}
	return nil, &net.DNSError{Err: "no suitable address found", Name: host}
}

type firewallRules interface {
	Exists(table, chain string, rulespec ...string) (bool, error)
	Append(table, chain string, rulespec ...string) error
	Delete(table, chain string, rulespec ...string) error
}

func installFirewallRule(f firewallRules, rule []string) (bool, error) {
	exists, err := f.Exists("filter", "OUTPUT", rule...)
	if err != nil {
		return false, fmt.Errorf("check firewall rule: %w", err)
	}
	if exists {
		return false, nil
	}
	if err = f.Append("filter", "OUTPUT", rule...); err != nil {
		return false, fmt.Errorf("add firewall rule: %w", err)
	}
	return true, nil
}
func deleteFirewallRule(f firewallRules, rule []string) error {
	if err := f.Delete("filter", "OUTPUT", rule...); err != nil {
		return fmt.Errorf("delete firewall rule: %w", err)
	}
	return nil
}

// Each listener owns a distinct rule, including wildcard listeners. This
// prevents one process from deleting another process's identical rule.
func listenerFirewallRule(ipv6 bool, ip net.IP, port int, owner string) []string {
	rule := []string{"-m", "ttl", "--ttl-eq", "1"}
	if ipv6 {
		rule = []string{"-m", "hl", "--hl-eq", "1"}
	}
	rule = append(rule, "-p", "tcp")
	if ip != nil && !ip.IsUnspecified() {
		rule = append(rule, "-s", ip.String())
	}
	return append(rule, "--sport", strconv.Itoa(port), "-m", "comment", "--comment", owner, "-j", "DROP")
}

func rawDialAddress(addr *net.TCPAddr) string {
	ip := net.IPAddr{IP: addr.IP, Zone: addr.Zone}
	return ip.String()
}

func flowSourceAddress(addr *net.IPAddr, port int, fallbackZone string) *net.TCPAddr {
	zone := addr.Zone
	if zone == "" && addr.IP.IsLinkLocalUnicast() {
		zone = fallbackZone
	}
	return &net.TCPAddr{IP: addr.IP, Port: port, Zone: zone}
}

func rawListenAddress(addr *net.TCPAddr) *net.IPAddr {
	return &net.IPAddr{IP: addr.IP, Zone: addr.Zone}
}

type contextFirewallRules interface {
	ExistsContext(context.Context, string, string, ...string) (bool, error)
	AppendContext(context.Context, string, string, ...string) error
}

func installFirewallRuleContext(ctx context.Context, f contextFirewallRules, rule []string) (bool, error) {
	exists, err := f.ExistsContext(ctx, "filter", "OUTPUT", rule...)
	if err != nil {
		return false, fmt.Errorf("check firewall rule: %w", err)
	}
	if exists {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err = f.AppendContext(ctx, "filter", "OUTPUT", rule...); err != nil {
		// The command may have installed the rule before reporting failure.
		// The caller must attempt cleanup even on an ambiguous failure.
		return true, fmt.Errorf("add firewall rule: %w", err)
	}
	return true, nil
}

func controlSocket(raw syscall.RawConn, operation func(uintptr) error) error {
	var operationErr error
	if err := raw.Control(func(fd uintptr) { operationErr = operation(fd) }); err != nil {
		return err
	}
	return operationErr
}

func prepareAcceptedSocket(conn io.Closer, configure func() error) error {
	if err := configure(); err != nil {
		return errors.Join(err, conn.Close())
	}
	return nil
}
