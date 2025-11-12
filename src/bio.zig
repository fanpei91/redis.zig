//! The design is trivial, we have a structure representing a job to perform
//! and a different thread and job queue for every job type.
//! Every thread waits for new jobs in its queue, and process every job
//! sequentially.
//!
//! Jobs of the same type are guaranteed to be processed from the least
//! recently inserted to the most recently inserted (older jobs processed
//! first).
//!
//! Currently there is no way for the creator of the job to be notified about
//! the completion of the operation, this will only be added when/if needed.

const NUM_OPS = 3;

var threads: [NUM_OPS]std.Thread = undefined;
var mutex: [NUM_OPS]std.Thread.Mutex = undefined;
var newjob_cond: [NUM_OPS]std.Thread.Condition = undefined;
var jobs: [NUM_OPS]*JobList = undefined;
var cancel: [NUM_OPS]std.atomic.Value(bool) = undefined;

pub fn init() std.Thread.SpawnError!void {
    mutex = .{ .{}, .{}, .{} };
    newjob_cond = .{ .{}, .{}, .{} };
    cancel = .{ .init(false), .init(false), .init(false) };
    for (0..NUM_OPS) |i| {
        jobs[i] = JobList.create(&.{ .free = Job.free });
        threads[i] = try std.Thread.spawn(
            .{
                .allocator = allocator.child,
            },
            processBackgroundJobs,
            .{
                @as(Job.Type, @enumFromInt(i)),
            },
        );
    }
}

fn processBackgroundJobs(job_type: Job.Type) void {
    const i: usize = @intFromEnum(job_type);

    const mtx: *std.Thread.Mutex = &mutex[i];
    const jbs: *JobList = jobs[i];
    const cond: *std.Thread.Condition = &newjob_cond[i];

    mtx.lock();
    defer mtx.unlock();

    while (!cancel[i].load(.seq_cst)) {
        if (jbs.len == 0) {
            cond.wait(mtx);
            continue;
        }
        const ln = jbs.first.?;
        defer jbs.removeNode(ln);
        const job = ln.value;
        job.do(job_type);
    }
}

pub fn createBackgroundJob(
    job_type: Job.Type,
    arg1: ?*anyopaque,
    arg2: ?*anyopaque,
    arg3: ?*anyopaque,
) void {
    const job = allocator.create(Job);
    job.arg1 = arg1;
    job.arg2 = arg2;
    job.arg3 = arg3;

    const i: usize = @intFromEnum(job_type);
    mutex[i].lock();
    defer mutex[i].unlock();

    jobs[i].append(job);
    newjob_cond[i].signal();
}

pub fn deinit() void {
    for (0..cancel.len) |i| cancel[i].store(true, .seq_cst);
    for (0..newjob_cond.len) |i| newjob_cond[i].signal();
    for (0..threads.len) |i| threads[i].join();
    for (jobs, 0..) |j, t| {
        var it = j.iterator(.forward);
        while (it.next()) |node| {
            node.value.do(@as(Job.Type, @enumFromInt(t)));
        }
        j.release();
    }
}

pub const Job = struct {
    /// Background job opcodes
    pub const Type = enum(u8) {
        closeFile = 0, // Deferred close(2) syscall.
        aofFsync = 1, // Deferred AOF fsync.
        lazyFree = 2, // Deferred objects freeing.
    };

    // Job specific arguments pointers. If we need to pass more than three
    // arguments we can just pass a pointer to a structure or alike.
    arg1: ?*anyopaque,
    arg2: ?*anyopaque,
    arg3: ?*anyopaque,

    fn do(job: *Job, job_type: Job.Type) void {
        switch (job_type) {
            .lazyFree => job.lazyFree(),
            // TODO: closeFile
            // TODO: aofFsync
            else => {},
        }
    }

    fn lazyFree(job: *Job) void {
        // What we free changes depending on what arguments are set:
        // arg1 -> free the object at pointer.
        // arg2 & arg3 -> free two dictionaries (a Redis DB).
        // only arg3 -> free the skiplist.
        if (job.arg1) |arg1| {
            lazyfree.freeObjectFromBioThread(
                @ptrCast(@alignCast(arg1)),
            );
            return;
        }
        if (job.arg2) |arg2| if (job.arg3) |arg3| {
            lazyfree.freeDatabaseFromBioThread(
                @ptrCast(@alignCast(arg2)),
                @ptrCast(@alignCast(arg3)),
            );
            return;
        };
        if (job.arg3) |arg3| {
            // TODO: free slots map
            _ = arg3;
            return;
        }
    }

    fn free(job: *Job) void {
        allocator.destroy(job);
    }
};

const std = @import("std");
const allocator = @import("allocator.zig");
const JobList = @import("adlist.zig").List(void, *Job);
const lazyfree = @import("lazyfree.zig");
const log = std.log.scoped(.bio);
