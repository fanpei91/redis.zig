const State = struct {
    kqfd: i32,
    events: []posix.Kevent,
};

pub fn create(el: *EventLoop) !void {
    const state = allocator.create(State);
    errdefer allocator.destroy(state);

    state.events = allocator.alloc(posix.Kevent, @intCast(el.getSetSize()));
    errdefer allocator.free(state.events);

    state.kqfd = try posix.kqueue();
    el.apidata = state;
}

pub fn addEvent(el: *EventLoop, fd: i32, mask: i32) !void {
    const state: *State = @ptrCast(@alignCast(el.apidata));

    var ke: posix.Kevent = .{
        .ident = @intCast(fd),
        .filter = 0,
        .flags = posix.system.EV.ADD,
        .fflags = 0,
        .data = 0,
        .udata = 0,
    };
    if (mask & ae.READABLE != 0) {
        ke.filter = posix.system.EVFILT.READ;
        _ = try posix.kevent(state.kqfd, &.{ke}, &.{}, null);
    }
    if (mask & ae.WRITABLE != 0) {
        ke.filter = posix.system.EVFILT.WRITE;
        _ = try posix.kevent(state.kqfd, &.{ke}, &.{}, null);
    }
}

pub fn delEvent(el: *EventLoop, fd: i32, mask: i32) !void {
    const state: *State = @ptrCast(@alignCast(el.apidata));

    var ke: posix.Kevent = .{
        .ident = @intCast(fd),
        .filter = 0,
        .flags = posix.system.EV.DELETE,
        .fflags = 0,
        .data = 0,
        .udata = 0,
    };
    if (mask & ae.READABLE != 0) {
        ke.filter = posix.system.EVFILT.READ;
        _ = try posix.kevent(state.kqfd, &.{ke}, &.{}, null);
    }
    if (mask & ae.WRITABLE != 0) {
        ke.filter = posix.system.EVFILT.WRITE;
        _ = try posix.kevent(state.kqfd, &.{ke}, &.{}, null);
    }
}

pub fn resize(el: *EventLoop, setsize: i32) void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    state.events = allocator.realloc(
        state.events,
        @intCast(setsize),
    );
}

pub fn poll(el: *EventLoop, timeout_in_ms: ?i64) !usize {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    var retval: usize = undefined;

    if (timeout_in_ms) |ms| {
        const timeout: posix.timespec = .{
            .sec = @divTrunc(ms, std.time.ms_per_s),
            .nsec = @rem(ms, std.time.ms_per_s) * std.time.ns_per_ms,
        };
        retval = try posix.kevent(state.kqfd, &.{}, state.events, &timeout);
    } else {
        retval = try posix.kevent(state.kqfd, &.{}, state.events, null);
    }

    var numevents: usize = 0;
    if (retval > 0) {
        numevents = retval;
        for (0..numevents) |i| {
            var mask: i32 = 0;
            const e = state.events[i];
            if (e.filter == posix.system.EVFILT.READ) mask |= ae.READABLE;
            if (e.filter == posix.system.EVFILT.WRITE) mask |= ae.WRITABLE;
            el.fired[i].fd = @intCast(e.ident);
            el.fired[i].mask = mask;
        }
    }
    return numevents;
}

pub fn name() []const u8 {
    return "kqueue";
}

pub fn free(el: *EventLoop) void {
    const state: *State = @ptrCast(@alignCast(el.apidata));
    posix.close(state.kqfd);
    allocator.free(state.events);
    allocator.destroy(state);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const ae = @import("ae.zig");
const EventLoop = ae.EventLoop;
const posix = std.posix;
