pub fn unixServer(
    path: []const u8,
    perm: posix.mode_t,
    backlog: u32,
) !posix.fd_t {
    const addr = try std.net.Address.initUnix(path);
    const socket = try posix.socket(
        addr.any.family,
        posix.SOCK.STREAM,
        0,
    );
    errdefer posix.close(socket);
    try posix.bind(socket, &addr.any, addr.getOsSockLen());
    try posix.listen(socket, @truncate(backlog));
    if (perm != 0) {
        try posix.fchmod(socket, perm);
    }
    return socket;
}

pub fn tcpServer(
    port: u16,
    bindaddr: []const u8,
    backlog: u32,
) !posix.fd_t {
    const addr = try std.net.Address.resolveIp(
        bindaddr,
        port,
    );
    const socket = try posix.socket(
        addr.any.family,
        posix.SOCK.STREAM,
        posix.IPPROTO.TCP,
    );
    errdefer posix.close(socket);
    try posix.setsockopt(
        socket,
        posix.SOL.SOCKET,
        posix.SO.REUSEADDR,
        &std.mem.toBytes(@as(c_int, 1)),
    );
    try posix.bind(socket, &addr.any, addr.getOsSockLen());
    try posix.listen(socket, @truncate(backlog));
    return socket;
}

pub fn enableTcpNoDelay(fd: posix.fd_t) !void {
    try posix.setsockopt(
        fd,
        posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        &std.mem.toBytes(@as(c_int, 1)),
    );
}

pub fn disableTcpNoDelay(fd: posix.fd_t) !void {
    try posix.setsockopt(
        fd,
        posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        &std.mem.toBytes(@as(c_int, 0)),
    );
}

/// Set TCP keep alive option to detect dead peers. The interval option
/// is only used for Linux as we are using Linux-specific APIs to set
/// the probe send time, interval, and count.
pub fn keepAlive(fd: posix.fd_t, interval: i32) !void {
    try posix.setsockopt(
        fd,
        posix.SOL.SOCKET,
        std.posix.SO.KEEPALIVE,
        &std.mem.toBytes(@as(c_int, 1)),
    );

    // Default settings are more or less garbage, with the keepalive time
    // set to 7200 by default on Linux. Modify settings to make the feature
    // actually useful.
    if (comptime builtin.os.tag == .linux) {
        // Send first probe after interval.
        var val = interval;
        try posix.setsockopt(
            fd,
            posix.IPPROTO.TCP,
            std.posix.TCP.KEEPIDLE,
            &std.mem.toBytes(@as(c_int, val)),
        );

        // Send next probes after the specified interval. Note that we set the
        // delay as interval / 3, as we send three probes before detecting
        // an error (see the next setsockopt call).
        val = @max(@divTrunc(val, 3), 1);
        try posix.setsockopt(
            fd,
            posix.IPPROTO.TCP,
            std.posix.TCP.KEEPINTVL,
            &std.mem.toBytes(@as(c_int, val)),
        );

        // Consider the socket in error state after three we send three ACK
        // probes without getting a reply.
        val = 3;
        try posix.setsockopt(
            fd,
            posix.IPPROTO.TCP,
            std.posix.TCP.KEEPCNT,
            &std.mem.toBytes(@as(c_int, val)),
        );
    }
}

pub fn nonBlock(fd: posix.fd_t) !void {
    const current_flags = try posix.fcntl(fd, posix.F.GETFL, 0);
    var new_flags: posix.O = @bitCast(@as(u32, @intCast(current_flags)));
    new_flags.NONBLOCK = true;
    const arg: u32 = @bitCast(new_flags);
    _ = try posix.fcntl(fd, posix.F.SETFL, arg);
}

pub const Socket = struct {
    addr: std.net.Address,
    fd: posix.fd_t,

    pub fn getPort(self: *const Socket) u16 {
        return self.addr.getPort();
    }

    pub fn getAddr(self: *const Socket, buffer: []u8) ![]u8 {
        var w = std.Io.Writer.fixed(buffer);
        try self.addr.format(&w);
        return w.buffered();
    }
};

pub fn accept(fd: posix.fd_t) posix.AcceptError!Socket {
    var sa: posix.sockaddr.storage = undefined;
    var salen: posix.socklen_t = @sizeOf(posix.sockaddr.storage);

    const cfd = try posix.accept(fd, @ptrCast(&sa), &salen, 0);
    const addr: *posix.sockaddr = @ptrCast(&sa);

    return .{
        .addr = .{ .any = addr.* },
        .fd = cfd,
    };
}

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");
