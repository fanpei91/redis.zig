//! Background I/O service for Redis.
//!
//! This file implements operations that we need to perform in the background.
//! Currently there is only a single operation, that is a background close(2)
//! system call. This is needed as when the process is the last owner of a
//! reference to a file closing it means unlinking it, and the deletion of the
//! file is slow, blocking the server.
//!
//! In the future we'll either continue implementing new things we need or
//! we'll switch to libeio. However there are probably long term uses for this
//! file as we may want to put here Redis specific background tasks (for instance
//! it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
//! implementation).
//!
//! DESIGN
//! ------
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
var pending: [NUM_OPS]u64 = undefined;

pub fn init() std.Thread.SpawnError!void {
    mutex = .{ .{}, .{}, .{} };
    newjob_cond = .{ .{}, .{}, .{} };
    pending = .{ 0, 0, 0 };
    for (0..NUM_OPS) |i| {
        jobs[i] = JobList.create(&.{ .freeVal = Job.free });
        threads[i] = try std.Thread.spawn(
            .{
                .allocator = allocator.impl,
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
    mutex[i].lock();
    const jbs: *JobList = jobs[i];
    const cond: *std.Thread.Condition = &newjob_cond[i];
    while (true) {
        if (jbs.len == 0) {
            cond.wait(&mutex[i]);
            continue;
        }
        const ln = jbs.first.?;
        const job = ln.value;
        mutex[i].unlock();
        job.do(job_type);
        mutex[i].lock();
        jbs.removeNode(ln);
        pending[i] -= 1;
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
    jobs[i].append(job);
    pending[i] += 1;
    newjob_cond[i].signal();
    mutex[i].unlock();
}

/// Return the number of pending jobs of the specified type.
pub fn pendingJobsOfType(job_type: Job.Type) u64 {
    const i: usize = @intFromEnum(job_type);
    mutex[i].lock();
    defer mutex[i].unlock();
    return pending[i];
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
            .lazyFree => {
                job.lazyFree();
            },
            .closeFile => {
                posix.close(@intCast(@intFromPtr(job.arg1.?)));
            },
            .aofFsync => {
                config.fsync(@intCast(@intFromPtr(job.arg1.?))) catch {};
            },
        }
    }

    fn lazyFree(job: *Job) void {
        // What we free changes depending on what arguments are set:
        // arg1 -> free the object at pointer.
        // arg2 & arg3 -> free two dictionaries (a Database).
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
const JobList = @import("list.zig").List(void, *Job);
const lazyfree = @import("lazyfree.zig");
const config = @import("config.zig");
const posix = std.posix;
