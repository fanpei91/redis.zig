/// Resolution in milliseconds
const RESOLUTION = 10;

/// Read the specified amount of bytes from 'fd'. If all the bytes are read
/// within 'timeout' milliseconds the operation succeed and 'size' is returned.
/// Otherwise the operation fails, error is returned, and an unspecified amount of
/// data could be read from the file descriptor.
pub fn read(
    fd: posix.fd_t,
    out: []u8,
    timeout: i64,
) !usize {
    if (out.len == 0) {
        return 0;
    }

    var totread: usize = 0;
    const start = std.time.milliTimestamp();
    var remaining = timeout;

    var ptr = out.ptr;
    var size = out.len;

    while (true) {
        const wait = @max(remaining, RESOLUTION);
        var nread: usize = 0;
        if (posix.read(fd, ptr[0..size])) |n| {
            if (n == 0) return error.ShortRead;
            nread = n;
        } else |err| {
            if (err != posix.ReadError.WouldBlock) {
                return err;
            }
        }

        ptr += nread;
        size -= nread;
        totread += nread;
        if (size == 0) return totread;

        // Wait
        _ = ae.wait(fd, ae.READABLE, wait) catch {};
        const elapsed = std.time.milliTimestamp() - start;
        if (elapsed >= timeout) {
            return error.TimeOut;
        }
        remaining = timeout - elapsed;
    }
}

const std = @import("std");
const posix = std.posix;
const ae = @import("ae.zig");
