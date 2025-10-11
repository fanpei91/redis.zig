const State = struct {
    epfd: i32,
    events: []linux.epoll_event,
};

pub fn create(allocator: Allocator, el: *EventLoop) !void {
    const state = try allocator.create(State);
    errdefer allocator.destroy(state);

    state.events = try allocator.alloc(linux.epoll_event, el.getSetSize());
    errdefer allocator.free(state.events);

    state.epfd = try posix.epoll_create1(0);
    el.apidata = state;
}

pub fn addEvent(el: *EventLoop, fd: int, mask: int) !void {
    const fi: usize = @intCast(fd);
    // If the fd was already monitored for some event, we need a MOD
    // operation. Otherwise we need an ADD operation.
    const op = if (el.events[fi].mask == ae.NONE)
        linux.EPOLL.CTL_ADD
    else
        linux.EPOLL.CTL_MOD;

    var msk = mask;
    msk |= el.events[fi].mask; // Merge old events;
    var ee: linux.epoll_event = .{
        .events = 0,
        .data = .{
            .fd = fd,
        },
    };
    if (msk & ae.READABLE != 0) ee.events |= linux.EPOLL.IN;
    if (msk & ae.WRITABLE != 0) ee.events |= linux.EPOLL.OUT;

    const state: *State = @ptrCast(@alignCast(el.apidata));
    try posix.epoll_ctl(state.epfd, op, fd, &ee);
}

pub fn delEvent(el: *EventLoop, fd: int, del_mask: int) !void {
    const mask = el.events[@intCast(fd)].mask & (~del_mask);
    var ee: linux.epoll_event = .{
        .events = 0,
        .data = .{
            .fd = fd,
        },
    };
    if (mask & ae.READABLE != 0) ee.events |= linux.EPOLL.IN;
    if (mask & ae.WRITABLE != 0) ee.events |= linux.EPOLL.OUT;

    const state: *State = @ptrCast(@alignCast(el.apidata));
    if (mask != ae.NONE) {
        try posix.epoll_ctl(state.epfd, linux.EPOLL.CTL_MOD, fd, &ee);
        return;
    }
    try posix.epoll_ctl(state.epfd, linux.EPOLL.CTL_DEL, fd, &ee);
}

pub fn resize(
    allocator: Allocator,
    el: *EventLoop,
    setsize: int,
) Allocator.Error!void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    state.events = try allocator.realloc(
        state.events,
        @intCast(setsize),
    );
}

pub fn poll(el: *EventLoop, timeout_in_ms: ?long) usize {
    const state: *State = @ptrCast(@alignCast(el.apidata));

    const timeout: i32 = @intCast(timeout_in_ms orelse -1);
    const ready = posix.epoll_wait(
        state.epfd,
        state.events,
        timeout,
    );
    if (ready > 0) {
        for (0..ready) |i| {
            var mask: int = 0;
            const e = state.events[i];

            if (e.events & linux.EPOLL.IN != 0) mask |= ae.READABLE;
            if (e.events & linux.EPOLL.OUT != 0) mask |= ae.WRITABLE;
            if (e.events & linux.EPOLL.ERR != 0) mask |= ae.WRITABLE;
            if (e.events & linux.EPOLL.HUP != 0) mask |= ae.WRITABLE;

            el.fired[i].fd = e.data.fd;
            el.fired[i].mask = mask;
        }
    }
    return ready;
}

pub fn name() []const u8 {
    return "epoll";
}

pub fn free(allocator: Allocator, el: *EventLoop) void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    posix.close(state.epfd);
    allocator.free(state.events);
    allocator.destroy(state);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const ae = @import("ae.zig");
const EventLoop = ae.EventLoop;
const ctypes = @import("ctypes.zig");
const int = ctypes.int;
const long = ctypes.long;
const linux = std.os.linux;
const posix = std.posix;
