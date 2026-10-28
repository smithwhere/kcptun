# Diagnosing sessions that stop forwarding

A whole smux session ending tears down its streams. Repeating AcceptStream on
that dead session does not reconnect KCP and must not replace the return in
server/main.go.

The vendored smux now reports `smux keepalive timeout` (a net.Error with
Timeout() == true) when its keepalive watchdog expires. Both a blocked accept
and subsequent accepts retain this cause even if transport closure also wakes
the receive loop. Explicit Close still reports io.ErrClosedPipe. No heartbeat
interval, timeout duration, framing, or retransmission settings are changed.

The server records local/remote addresses, heartbeat interval and watchdog
timeout on session startup, and addresses, lifetime, stream count and error
on session termination. These records remain enabled with --quiet. A logged
keepalive timeout identifies the watchdog that closed the session, not the
underlying reason traffic stopped reaching it.

The KCP regression comparison workflow builds two **diagnostic**, Linux amd64
client/server artifact sets using Go 1.27.3: KCP v5.6.73 and v5.6.66. Only the
vendored KCP source and its version markers change in the baseline job; the
smux changes, application code and other dependencies remain identical.
The baseline source is pinned to the v20261018 commit, not a moving tag.

Download artifacts from the PR's KCP regression comparison workflow run.
Both jobs must pass race tests and the real tunnel EOF/half-close suite before
publishing artifacts. SHA256SUMS and build-info.txt identify the binaries.
These artifacts do not create a release or deploy to a VPS.

For a live comparison, retain the working binary and run one variant at a time
with the same configuration, client, server and network. Restarting a service
interrupts its existing sessions. Record timezones and session-end records.
If only one variant fails, that narrows the regression to KCP or its interaction
with the rest of the stack; it is not proof of a specific upstream defect.
