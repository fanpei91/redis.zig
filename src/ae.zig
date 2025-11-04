/// No events registered.
pub const NONE = 0;
/// Fire when descriptor is readable.
pub const READABLE = 1;
/// Fire when descriptor is writable.
pub const WRITABLE = 2;
/// With WRITABLE, never fire the event if the
/// READABLE event already fired in the same event
/// loop iteration. Useful when you want to persist
/// things to disk before sending replies, and want
/// to do that in a group fashion.
pub const BARRIER = 4;

const FILE_EVENTS = 1;
const TIME_EVENTS = 2;
const ALL_EVENTS = FILE_EVENTS | TIME_EVENTS;
const DONT_WAIT = 4;
const CALL_AFTER_SLEEP = 8;

const NOMORE = -1;
const DELETED_EVENT_ID = -1;

pub const ClientData = ?*anyopaque;

pub const FileEvent = struct {
    /// one of (READABLE|WRITABLE|BARRIER)
    mask: i32,
    rfileProc: FileProc,
    wfileProc: FileProc,
    client_data: ClientData,
};

pub const FiredEvent = struct {
    fd: i32,
    mask: i32,
};

pub const TimeEvent = struct {
    id: i64,
    when_sec: i64,
    when_ms: i64,
    timeProc: TimeProc,
    finalizerProc: ?EventFinalizerProc,
    client_data: ClientData,
    prev: ?*TimeEvent,
    next: ?*TimeEvent,
};

pub const FileProc = *const fn (
    Allocator,
    *EventLoop,
    fd: i32,
    client_data: ClientData,
    mask: i32,
) anyerror!void;

pub const TimeProc = *const fn (
    Allocator,
    *EventLoop,
    id: i64,
    clicent_data: ClientData,
) anyerror!i32;

pub const EventFinalizerProc = *const fn (
    Allocator,
    *EventLoop,
    clicent_data: ClientData,
) anyerror!void;

pub const BeforeSleepProc = *const fn (
    Allocator,
    *EventLoop,
) anyerror!void;

pub const EventLoop = struct {
    allocator: Allocator,
    /// highest file descriptor currently registered
    maxfd: i32,
    /// This is used for polling API specific data
    apidata: *anyopaque,
    /// Registered events
    events: []FileEvent,
    /// Fired events
    fired: []FiredEvent,
    /// Used to detect system clock skew
    last_time: i64,
    time_event_head: ?*TimeEvent,
    time_event_next_id: i64,
    stopped: bool,
    beforeSleep: ?BeforeSleepProc,
    afterSleep: ?BeforeSleepProc,

    pub fn create(
        allocator: Allocator,
        setsize: i32,
    ) !*EventLoop {
        std.debug.assert(setsize > 0);

        const el = try allocator.create(EventLoop);
        errdefer allocator.destroy(el);
        el.allocator = allocator;

        el.events = try allocator.alloc(FileEvent, @intCast(setsize));
        errdefer allocator.free(el.events);

        el.fired = try allocator.alloc(FiredEvent, @intCast(setsize));
        errdefer allocator.free(el.fired);

        el.last_time = std.time.timestamp();
        el.time_event_head = null;
        el.time_event_next_id = 0;
        el.stopped = false;
        el.maxfd = -1;
        el.beforeSleep = null;
        el.afterSleep = null;

        try api.create(allocator, el);
        for (0..el.events.len) |i| {
            el.events[i].mask = NONE;
        }
        return el;
    }

    /// Resize the maximum set size of the event loop.
    /// If the requested set size is smaller than the current set size, but
    /// there is already a file descriptor in use that is >= the requested
    /// set size minus one, false is returned and the operation is not
    /// performed at all.
    ///
    /// Otherwise true is returned and the operation is successful.
    pub fn resizeSetSize(
        self: *EventLoop,
        setsize: i32,
    ) !bool {
        if (setsize == self.events.len) return true;
        if (self.maxfd >= setsize) return false;

        try api.resize(self.allocator, self, setsize);
        self.events = try self.allocator.realloc(self.events, @intCast(setsize));
        self.fired = try self.allocator.realloc(self.fired, @intCast(setsize));

        // Make sure that if we created new slots, they are initialized with
        // an NONE mask.
        var i = self.maxfd + 1;
        while (i < setsize) : (i += 1) {
            self.events[i].mask = NONE;
        }

        return true;
    }

    pub fn getSetSize(self: *EventLoop) i32 {
        return @intCast(self.events.len);
    }

    pub fn getApiName() []const u8 {
        return api.name();
    }

    pub fn stop(self: *EventLoop) void {
        self.stopped = true;
    }

    pub fn createFileEvent(
        self: *EventLoop,
        fd: i32,
        mask: i32,
        proc: FileProc,
        client_data: ClientData,
    ) !void {
        if (fd >= self.getSetSize()) return error.InvalidFileDescriptor;
        try api.addEvent(self, fd, mask);

        const fe: *FileEvent = &self.events[@intCast(fd)];
        fe.mask |= mask;
        if (mask & READABLE != 0) fe.rfileProc = proc;
        if (mask & WRITABLE != 0) fe.wfileProc = proc;
        fe.client_data = client_data;

        if (fd > self.maxfd) self.maxfd = fd;
    }

    pub fn deleteFileEvent(self: *EventLoop, fd: i32, mask: i32) !void {
        if (fd >= self.getSetSize()) return;

        const fe: *FileEvent = &self.events[@intCast(fd)];
        if (fe.mask == NONE) return;

        var msk = mask;

        // We want to always remove BARRIER if set when WRITABLE
        // is removed.
        if (msk & WRITABLE != 0) msk |= BARRIER;

        try api.delEvent(self, fd, msk);
        fe.mask = fe.mask & (~msk);
        if (fd == self.maxfd and fe.mask == NONE) {
            var j = self.maxfd - 1;
            while (j >= 0) : (j -= 1) {
                if (self.events[@intCast(j)].mask != NONE) break;
            }
            self.maxfd = j;
        }
    }

    pub fn getFileEvents(self: *EventLoop, fd: i32) i32 {
        if (fd >= self.getSetSize()) return 0;
        return self.events[@intCast(fd)].mask;
    }

    pub fn createTimeEvent(
        self: *EventLoop,
        milliseconds: i64,
        proc: TimeProc,
        client_data: ClientData,
        finalizerProc: ?EventFinalizerProc,
    ) !i64 {
        const id = self.time_event_next_id;
        self.time_event_next_id += 1;

        const te = try self.allocator.create(TimeEvent);
        te.id = id;
        addMillisecondsToNow(milliseconds, &te.when_sec, &te.when_ms);
        te.timeProc = proc;
        te.finalizerProc = finalizerProc;
        te.client_data = client_data;
        te.prev = null;
        te.next = self.time_event_head;

        if (te.next) |next| {
            next.prev = te;
        }
        self.time_event_head = te;

        return id;
    }

    pub fn deleteTimeEvent(self: *EventLoop, id: i64) bool {
        var te = self.time_event_head;
        while (te) |event| {
            if (event.id == id) {
                event.id = DELETED_EVENT_ID;
                return true;
            }
            te = event.next;
        }
        return false;
    }

    /// Process every pending time event, then every pending file event
    /// (that may be registered by time event callbacks just processed).
    /// Without special flags the function sleeps until some file event
    /// fires, or when the next time event occurs (if any).
    ///
    /// 1) If flags is 0, the function does nothing and returns.
    /// 2) if flags has ALL_EVENTS set, all the kind of events are processed.
    /// 3) if flags has FILE_EVENTS set, file events are processed.
    /// 4) if flags has TIME_EVENTS set, time events are processed.
    /// 5) if flags has DONT_WAIT set, the function returns ASAP until all
    ///  the events that's possible to process without to wait are processed.
    /// 6) if flags has CALL_AFTER_SLEEP set, the aftersleep callback is called.
    ///
    /// The function returns the number of events processed.
    pub fn processEvents(
        self: *EventLoop,
        flags: i32,
    ) !usize {
        var processed: usize = 0;

        // Nothing to do? return ASAP
        if ((flags & TIME_EVENTS == 0) and
            (flags & FILE_EVENTS == 0))
        {
            return 0;
        }

        // Note that we want call select() even if there are no
        // file events to process as long as we want to process time
        // events, in order to sleep until the next time event is ready
        // to fire.
        const waitting = flags & DONT_WAIT == 0;
        if (self.maxfd != -1 or (flags & TIME_EVENTS != 0 and waitting)) {
            var shortest: ?*TimeEvent = null;
            if (flags & TIME_EVENTS != 0 and waitting) {
                shortest = self.searchNearestTimer();
            }
            var timeout: ?i64 = null;
            if (shortest) |st| {
                const now = getTime();
                // How many milliseconds we need to wait for the next
                // time event to fire?
                const ms = (st.when_sec - now.sec) * 1000 + st.when_ms - now.ms;
                if (ms > 0) {
                    timeout = ms;
                } else {
                    timeout = 0;
                }
            } else {
                if (!waitting) {
                    timeout = 0;
                } else {
                    timeout = null; // wait forever
                }
            }

            const numevents: usize = try api.poll(self, timeout);

            if (self.afterSleep != null and flags & CALL_AFTER_SLEEP != 0) {
                try self.afterSleep.?(self.allocator, self);
            }

            for (0..numevents) |i| {
                const mask = self.fired[i].mask;
                const fd = self.fired[i].fd;
                const fe: *FileEvent = &self.events[@intCast(fd)];

                // Number of events fired for current fd.
                var fired: usize = 0;

                // Normally we execute the readable event first, and the writable
                // event laster. This is useful as sometimes we may be able
                // to serve the reply of a query immediately after processing the
                // query.
                //
                // However if BARRIER is set in the mask, our application is
                // asking us to do the reverse: never fire the writable event
                // after the readable. In such a case, we invert the calls.
                // This is useful when, for instance, we want to do things
                // in the beforeSleep() hook, like fsynching a file to disk,
                // before replying to a client.
                const invert = fe.mask & BARRIER != 0;

                // Note the "fe->mask & mask & ..." code: maybe an already
                // processed event removed an element that fired and we still
                // didn't processed, so we check if the event is still valid.
                //
                // Fire the readable event if the call sequence is not
                // inverted.
                if (!invert and fe.mask & mask & READABLE != 0) {
                    try fe.rfileProc(
                        self.allocator,
                        self,
                        fd,
                        fe.client_data,
                        mask,
                    );
                    fired += 1;
                }

                // Fire the writable event.
                if (fe.mask & mask & WRITABLE != 0) {
                    if (fired == 0 or fe.wfileProc != fe.rfileProc) {
                        try fe.wfileProc(
                            self.allocator,
                            self,
                            fd,
                            fe.client_data,
                            mask,
                        );
                        fired += 1;
                    }
                }

                // If we have to invert the call, fire the readable event now
                // after the writable one.
                if (invert and fe.mask & mask & READABLE != 0) {
                    if (fired == 0 or fe.wfileProc != fe.rfileProc) {
                        try fe.rfileProc(
                            self.allocator,
                            self,
                            fd,
                            fe.client_data,
                            mask,
                        );
                        fired += 1;
                    }
                }

                processed += 1;
            }
        }

        // Check time events
        if (flags & TIME_EVENTS != 0) {
            processed += try self.processTimeEvents();
        }

        return processed;
    }

    pub fn main(self: *EventLoop) !void {
        self.stopped = false;
        while (!self.stopped) {
            if (self.beforeSleep) |beforeSleep| {
                try beforeSleep(self.allocator, self);
            }
            _ = try self.processEvents(ALL_EVENTS | CALL_AFTER_SLEEP);
        }
    }

    pub fn setBeforeSleepProc(
        self: *EventLoop,
        beforeSleep: BeforeSleepProc,
    ) void {
        self.beforeSleep = beforeSleep;
    }

    pub fn setAfterSleepProc(
        self: *EventLoop,
        afterSleep: BeforeSleepProc,
    ) void {
        self.afterSleep = afterSleep;
    }

    pub fn destroy(self: *EventLoop) void {
        api.free(self.allocator, self);
        self.allocator.free(self.events);
        self.allocator.free(self.fired);
        self.allocator.destroy(self);
    }

    /// Search the first timer to fire.
    /// This operation is useful to know how many time the select can be
    /// put in sleep without to delay any event.
    /// If there are no timers NULL is returned.
    ///
    /// Note that's O(N) since time events are unsorted.
    /// Possible optimizations (not needed by Redis so far, but...):
    /// 1) Insert the event in order, so that the nearest is just the head.
    ///    Much better but still insertion or deletion of timers is O(N).
    /// 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
    fn searchNearestTimer(self: *EventLoop) ?*TimeEvent {
        var curr = self.time_event_head;
        var nearest: ?*TimeEvent = null;
        while (curr) |te| {
            if (nearest == null or te.when_sec < nearest.?.when_sec or
                (te.when_sec == nearest.?.when_sec and
                    te.when_ms < nearest.?.when_ms))
            {
                nearest = te;
            }
            curr = te.next;
        }
        return nearest;
    }

    fn processTimeEvents(self: *EventLoop) !usize {
        // If the system clock is moved to the future, and then set back to the
        // right value, time events may be delayed in a random way. Often this
        // means that scheduled operations will not be performed soon enough.
        //
        // Here we try to detect system clock skews, and force all the time
        // events to be processed ASAP when this happens: the idea is that
        // processing events earlier is less dangerous than delaying them
        // indefinitely, and practice suggests it is.
        const curr_ts = std.time.timestamp();
        if (curr_ts < self.last_time) {
            var curr_event = self.time_event_head;
            while (curr_event) |te| {
                te.when_sec = 0;
                curr_event = te.next;
            }
        }
        self.last_time = curr_ts;

        const max_id = self.time_event_next_id - 1;
        var processed: usize = 0;
        var curr_event = self.time_event_head;
        while (curr_event) |te| {
            if (te.id == DELETED_EVENT_ID) {
                if (te.prev) |prev| {
                    prev.next = te.next;
                } else {
                    self.time_event_head = te.next;
                }
                if (te.next) |next| {
                    next.prev = te.prev;
                }
                if (te.finalizerProc) |finalizer| {
                    try finalizer(self.allocator, self, te.client_data);
                }
                curr_event = te.next;
                self.allocator.destroy(te);
                continue;
            }

            // Make sure we don't process time events created by time events in
            // this iteration. Note that this check is currently useless: we always
            // add new timers on the head, however if we change the implementation
            // detail, this check may be useful again: we keep it here for future
            // defense.
            if (te.id > max_id) {
                curr_event = te.next;
                continue;
            }

            const now = getTime();
            if (now.sec > te.when_sec or
                (now.sec == te.when_sec and now.ms > te.when_ms))
            {
                const retval = try te.timeProc(
                    self.allocator,
                    self,
                    te.id,
                    te.client_data,
                );
                processed += 1;
                if (retval != NOMORE) {
                    addMillisecondsToNow(retval, &te.when_sec, &te.when_ms);
                } else {
                    te.id = DELETED_EVENT_ID;
                }
            }
            curr_event = te.next;
        }
        return processed;
    }
};

/// Wait for milliseconds until the given file descriptor becomes
/// writable/readable/exception
pub fn wait(fd: i32, mask: i32, milliseconds: i64) posix.PollError!usize {
    var pfd: posix.pollfd = undefined;
    @memset(std.mem.asBytes(&pfd), 0);
    pfd.fd = @intCast(fd);

    if (mask & READABLE != 0) pfd.events |= posix.POLL.IN;
    if (mask & WRITABLE != 0) pfd.events |= posix.POLL.OUT;

    var retmask: usize = 0;
    var fds = [_]posix.pollfd{pfd};
    const ready = try posix.poll(&fds, @truncate(milliseconds));
    if (ready == 1) {
        if (pfd.revents & posix.POLL.IN != 0) retmask |= READABLE;
        if (pfd.revents & posix.POLL.OUT != 0) retmask |= WRITABLE;
        if (pfd.revents & posix.POLL.ERR != 0) retmask |= WRITABLE;
        if (pfd.revents & posix.POLL.HUP != 0) retmask |= WRITABLE;
        return retmask;
    }
    return ready;
}

fn addMillisecondsToNow(milliseconds: i64, sec: *i64, ms: *i64) void {
    const now = getTime();
    var when_sec = now.sec + @divTrunc(milliseconds, std.time.ms_per_s);
    var when_ms = now.ms + @rem(milliseconds, std.time.ms_per_s);
    if (when_ms >= std.time.ms_per_s) {
        when_sec += 1;
        when_ms -= std.time.ms_per_s;
    }
    sec.* = when_sec;
    ms.* = when_ms;
}

fn getTime() struct { sec: i64, ms: i64 } {
    const timestamp = std.time.milliTimestamp();
    const sec = @divTrunc(timestamp, std.time.ms_per_s);
    const ms = @rem(timestamp, std.time.ms_per_s);
    return .{
        .sec = sec,
        .ms = ms,
    };
}

test addMillisecondsToNow {
    var when_sec: i64 = 0;
    var when_ms: i64 = 0;
    addMillisecondsToNow(10 * std.time.ns_per_s, &when_sec, &when_ms);
    std.debug.print("sec: {}, ms: {}\n", .{ when_sec, when_ms });
}

test wait {
    const ret = try wait(0, READABLE | WRITABLE, 10);
    std.debug.print("{}\n", .{ret});
}

const builtin = @import("builtin");
const std = @import("std");
const posix = std.posix;
const expect = std.testing.expect;
const Allocator = std.mem.Allocator;
const api = switch (builtin.os.tag) {
    .linux => @import("ae_epoll.zig"),
    .macos, .netbsd, .freebsd, .dragonfly, .openbsd => @import("ae_kqueue.zig"),
    else => @import("ae_select.zig"),
};
