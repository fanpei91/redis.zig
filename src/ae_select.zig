const State = struct {
    rfds: c.fd_set,
    wfds: c.fd_set,
    // We need to have a copy of the fd sets as it's not safe to reuse
    // FD sets after select().
    _rfds: c.fd_set,
    _wfds: c.fd_set,
};

pub fn create(el: *EventLoop) !void {
    const state = allocator.create(State);

    c.FD_ZERO(&state.rfds);
    c.FD_ZERO(&state.wfds);
    el.apidata = state;
}

pub fn addEvent(el: *EventLoop, fd: i32, mask: i32) !void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    if (mask & ae.READABLE != 0) c.FD_SET(fd, &state.rfds);
    if (mask & ae.WRITABLE != 0) c.FD_SET(fd, &state.wfds);
}

pub fn delEvent(el: *EventLoop, fd: i32, mask: i32) !void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    if (mask & ae.READABLE != 0) c.FD_CLR(fd, &state.rfds);
    if (mask & ae.WRITABLE != 0) c.FD_CLR(fd, &state.wfds);
}

pub fn resize(_: *EventLoop, _: i32) void {}

pub fn poll(el: *EventLoop, timeout_in_ms: ?i64) !usize {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    @memcpy(
        std.mem.toBytes(&state._rfds),
        std.mem.toBytes(&state.rfds),
    );
    @memcpy(
        std.mem.toBytes(&state._wfds),
        std.mem.toBytes(&state.wfds),
    );
    var timeout: c.timeval = undefined;
    if (timeout_in_ms) |ms| {
        timeout = .{
            .tv_sec = @divTrunc(ms, std.time.ms_per_s),
            .tv_usec = @rem(ms, std.time.ms_per_s) * std.time.us_per_ms,
        };
    }
    const retval: i32 = c.select(
        el.maxfd + 1,
        &state._rfds,
        &state._wfds,
        null,
        if (timeout_in_ms != null) &timeout else null,
    );

    var numevents: usize = 0;
    if (retval > 0) {
        var i: usize = 0;
        const maxfd: usize = @intCast(el.maxfd);
        while (i <= maxfd) : (i += 1) {
            var mask: i32 = 0;
            const fe = el.events[i];
            if (fe.mask == ae.NONE) continue;
            if (fe.mask & ae.READABLE != 0 and c.FD_ISSET(i, &state._rfds) != 0) {
                mask |= ae.READABLE;
            }
            if (fe.mask & ae.WRITABLE != 0 and c.FD_ISSET(i, &state._wfds) != 0) {
                mask |= ae.WRITABLE;
            }
            el.fired[numevents].fd = @intCast(i);
            el.fired[numevents].mask = mask;
            numevents += 1;
        }
    }
    return numevents;
}

pub fn name() []const u8 {
    return "select";
}

pub fn free(el: *EventLoop) void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    allocator.destroy(state);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const ae = @import("ae.zig");
const EventLoop = ae.EventLoop;
const c = @cImport({
    @cInclude("sys/select.h");
});
