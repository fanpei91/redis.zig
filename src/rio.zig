//! A simple stream-oriented I/O abstraction that provides an interface
//! to write code that can consume/produce data using different concrete input
//! and output devices. For instance the same rdb.zig code using the rio
//! abstraction can be used to read and write the RDB format using in-memory
//! buffers or files.
//!
//! A rio object provides the following methods:
//!  read: read from stream.
//!  write: write to stream.
//!  tell: get the current offset.
//!
//! It is also possible to set a 'checksum' method that is used by rio.zig in order
//! to compute a checksum of the data written or read, or to query the rio object
//! for the current checksum.

const Error = error{
    Unsupported,
    Timeout,
} || std.Io.Reader.Error ||
    std.Io.Writer.Error ||
    std.fs.File.GetSeekPosError ||
    std.fs.File.SyncError ||
    posix.WriteError;

pub const ReadWriter = struct {
    pub const VTable = struct {
        read: *const fn (*anyopaque, []u8) Error!void,
        write: *const fn (*anyopaque, []const u8) Error!void,
        flush: *const fn (*anyopaque) Error!void,
        tell: *const fn (*anyopaque) Error!usize,
    };
    /// The updateChecksum method if not NULL is used to compute the checksum of
    /// all the data that was read or written so far. The method should be
    /// designed so that can be called with the current checksum, and the buf
    /// pointing to the new block of data to add to the checksum computation.
    updateChecksum: ?*const fn (*ReadWriter, []const u8) void = null,

    ptr: *anyopaque,
    vtable: *const VTable,

    /// The current checksum
    cksum: std.hash.crc.Crc64Redis,

    /// number of bytes read or written
    processed_bytes: usize,

    /// maximum single read or write chunk size
    max_processing_chunk: usize,

    pub fn write(self: *ReadWriter, buf: []const u8) Error!void {
        var ptr = buf.ptr;
        var len = buf.len;

        while (len > 0) {
            const bytes_to_write = if (self.max_processing_chunk != 0 and
                self.max_processing_chunk < len)
                self.max_processing_chunk
            else
                len;
            const data = ptr[0..bytes_to_write];
            if (self.updateChecksum) |checksum| {
                checksum(self, data);
            }
            try self.vtable.write(self.ptr, data);
            ptr += data.len;
            len -= data.len;
            self.processed_bytes +%= data.len;
        }
    }

    pub fn read(self: *ReadWriter, buf: []u8) Error!void {
        var ptr = buf.ptr;
        var len = buf.len;
        while (len > 0) {
            const bytes_to_read = if (self.max_processing_chunk != 0 and
                self.max_processing_chunk < len)
                self.max_processing_chunk
            else
                len;
            const data = ptr[0..bytes_to_read];
            try self.vtable.read(self.ptr, data);
            if (self.updateChecksum) |checksum| {
                checksum(self, data);
            }
            ptr += data.len;
            len -= data.len;
            self.processed_bytes +%= data.len;
        }
    }

    pub fn tell(self: *ReadWriter) Error!usize {
        return self.vtable.tell(self.ptr);
    }

    pub fn flush(self: *ReadWriter) Error!void {
        return self.vtable.flush(self.ptr);
    }

    // --------------------------- Higher level interface --------------------------
    // The following higher level functions use lower level rio.c functions to help
    // generating the Redis protocol for the Append Only File.

    /// Write multi bulk count in the format: "*<count>\r\n".
    pub fn writeBulkCount(
        self: *ReadWriter,
        prefix: u8,
        count: i64,
    ) Error!usize {
        var cbuf: [128]u8 = undefined;

        cbuf[0] = prefix;
        const llval = util.ll2string(cbuf[1..], count);
        cbuf[llval.len + 1] = '\r';
        cbuf[llval.len + 2] = '\n';
        const clen = 1 + llval.len + 2;
        try self.write(cbuf[0..clen]);
        return clen;
    }

    /// Write binary-safe string in the format: "$<count>\r\n<payload>\r\n".
    pub fn writeBulkString(self: *ReadWriter, buf: []const u8) Error!usize {
        const nwritten = self.writeBulkCount('$', buf.len);
        if (buf.len > 0) {
            self.write(buf) catch {
                return 0;
            };
        }
        try self.write("\r\n");
        return nwritten + buf.len + 2;
    }

    /// Write a long long value in format: "$<count>\r\n<payload>\r\n".
    pub fn writeBulkLongLong(self: *ReadWriter, l: i64) Error!usize {
        var lbuf: [32]u8 = undefined;
        const str = util.ll2string(&lbuf, l);
        return self.writeBulkString(str);
    }

    /// Write a double value in the format: "$<count>\r\n<payload>\r\n"
    pub fn writeBulkDouble(self: *ReadWriter, d: f64) Error!usize {
        var dbuf: [128]u8 = undefined;
        const str = util.d2string(&dbuf, d);
        return self.writeBulkString(str);
    }
};

pub const FileReadWriter = struct {
    file: std.fs.File,
    rbuf: [8192]u8,
    reader: std.fs.File.Reader,
    wbuf: [8192]u8,
    writer: std.fs.File.Writer,
    /// Bytes written since last fsync.
    buffered: usize,
    /// sync after 'autosync' bytes written.
    autosync: usize,

    pub fn init(self: *FileReadWriter, file: std.fs.File) void {
        self.file = file;
        self.reader = file.reader(&self.rbuf);
        self.writer = file.writer(&self.wbuf);
        self.buffered = 0;
        self.autosync = 0;
    }

    pub fn readWriter(self: *FileReadWriter) ReadWriter {
        return .{
            .ptr = self,
            .vtable = &.{
                .read = read,
                .write = write,
                .flush = flush,
                .tell = tell,
            },
            .cksum = .init(),
            .processed_bytes = 0,
            .max_processing_chunk = 0,
        };
    }

    /// Set the file-based rio object to auto-fsync every 'bytes' file written.
    /// By default this is set to zero that means no automatic file sync is
    /// performed.
    ///
    /// This feature is useful in a few contexts since when we rely on OS write
    /// buffers sometimes the OS buffers way too much, resulting in too many
    /// disk I/O concentrated in very little time. When we fsync in an explicit
    /// way instead the I/O pressure is more distributed across time.
    pub fn setAutoSync(self: *FileReadWriter, bytes: usize) void {
        self.autosync = bytes;
    }

    fn read(ptr: *anyopaque, buf: []u8) Error!void {
        const self: *FileReadWriter = @ptrCast(@alignCast(ptr));
        try self.reader.interface.readSliceAll(buf);
    }

    fn write(ptr: *anyopaque, buf: []const u8) Error!void {
        const self: *FileReadWriter = @ptrCast(@alignCast(ptr));
        try self.writer.interface.writeAll(buf);
        self.buffered += buf.len;
        if (self.autosync > 0 and self.buffered >= self.autosync) {
            try self.writer.interface.flush();
            try self.file.sync();
            self.buffered = 0;
        }
    }

    fn flush(ptr: *anyopaque) Error!void {
        const self: *FileReadWriter = @ptrCast(@alignCast(ptr));
        try self.writer.interface.flush();
    }

    fn tell(ptr: *anyopaque) Error!usize {
        const self: *FileReadWriter = @ptrCast(@alignCast(ptr));
        return try self.file.getPos();
    }
};

pub const BufferReadWriter = struct {
    ptr: sds.String,
    pos: usize,

    pub fn init(self: *BufferReadWriter, s: sds.String) void {
        self.ptr = s;
        self.pos = 0;
    }

    pub fn readWriter(self: *BufferReadWriter) ReadWriter {
        return .{
            .ptr = self,
            .vtable = &.{
                .read = read,
                .write = write,
                .flush = flush,
                .tell = tell,
            },
            .cksum = .init(),
            .processed_bytes = 0,
            .max_processing_chunk = 0,
        };
    }

    fn read(ptr: *anyopaque, buf: []u8) Error!void {
        const self: *BufferReadWriter = @ptrCast(@alignCast(ptr));
        if (sds.getLen(self.ptr) - self.pos < buf.len) {
            // not enough buffer to return len bytes.
            return Error.EndOfStream;
        }
        memcpy(buf, self.ptr + self.pos, buf.len);
        self.pos += buf.len;
    }

    fn write(ptr: *anyopaque, buf: []const u8) Error!void {
        const self: *BufferReadWriter = @ptrCast(@alignCast(ptr));
        self.ptr = sds.cat(allocator.child, self.ptr, buf);
        self.pos += buf.len;
    }

    fn flush(ptr: *anyopaque) Error!void {
        // Nothing to do, our write just appends to the buffer.
        _ = ptr;
    }

    fn tell(ptr: *anyopaque) Error!usize {
        const self: *BufferReadWriter = @ptrCast(@alignCast(ptr));
        return self.pos;
    }

    pub fn deinit(self: *BufferReadWriter) void {
        sds.free(allocator.child, self.ptr);
    }
};

pub const FdsetReadWriter = struct {
    /// File descriptors.
    fds: []posix.fd_t,
    /// Error state of each fd. 0 (if ok) or errno.
    state: []?Error,
    buf: sds.String,
    pos: usize,

    pub fn init(self: *FdsetReadWriter, fds: []posix.fd_t) void {
        self.fds = allocator.alloc(posix.fd_t, fds.len);
        memcpy(self.fds, fds, fds.len);
        self.state = allocator.alloc(?Error, fds.len);
        @memset(self.state, null);
        self.buf = sds.empty(allocator.child);
        self.pos = 0;
    }

    pub fn readWriter(self: *FdsetReadWriter) ReadWriter {
        return .{
            .ptr = self,
            .vtable = &.{
                .read = read,
                .write = write,
                .flush = flush,
                .tell = tell,
            },
            .cksum = .init(),
            .processed_bytes = 0,
            .max_processing_chunk = 0,
        };
    }

    fn read(ptr: *anyopaque, buf: []u8) Error!void {
        _ = ptr;
        _ = buf;
        // Error, this target does not support reading.
        return Error.Unsupported;
    }

    /// The function returns TRUE as long as we are able to correctly write
    /// to at least one file descriptor.
    ///
    /// When the length of buf is 0, the function performs a flush operation
    /// if there is some pending buffer, so this function is also used in order
    /// to implement flush().
    fn write(ptr: *anyopaque, buf: []const u8) Error!void {
        const self: *FdsetReadWriter = @ptrCast(@alignCast(ptr));
        var doflush = buf.len == 0;

        var p = buf.ptr;
        var len = buf.len;

        // To start we always append to our buffer. If it gets larger than
        // a given size, we actually write to the sockets.

        if (len > 0) {
            self.buf = sds.cat(allocator.child, self.buf, buf);
            len = 0; // Prevent entering the while below if we don't flush.
            if (sds.getLen(self.buf) > Server.PROTO_IOBUF_LEN) {
                doflush = true;
            }
        }

        if (doflush) {
            p = self.buf;
            len = sds.getLen(self.buf);
        }

        // Write in little chunchs so that when there are big writes we
        // parallelize while the kernel is sending data in background to
        // the TCP socket.
        while (len > 0) {
            const count = @min(1024, len);
            var broken: usize = 0;
            for (self.fds, 0..) |fd, i| {
                if (self.state[i] != null) {
                    // Skip FDs alraedy in error.
                    broken += 1;
                    continue;
                }
                // Make sure to write 'count' bytes to the socket regardless
                // of short writes.
                var nwritten: usize = 0;
                var err: ?Error = null;
                while (nwritten != count) {
                    const data = (p + nwritten)[0 .. count - nwritten];
                    nwritten += try posix.write(fd, data) catch |e| {
                        // With blocking sockets, which is the sole user of this
                        // rio target, WouldBlock is returned only because of
                        // the SO_SNDTIMEO socket option, so we translate the error
                        // into one more recognizable by the user.
                        err = e;
                        if (e == Error.WouldBlock) {
                            err = Error.Timeout;
                        }
                        break;
                    };
                }
                if (nwritten != count) {
                    // Mark this FD as broken.
                    self.state[i] = err;
                }
            }
            if (broken == self.fds.len) {
                // All the FDs in error.
                return Error.WriteFailed;
            }
            p += count;
            len -= count;
            self.pos += count;
        }
        if (doflush) {
            sds.clear(self.buf);
        }
    }

    fn flush(ptr: *anyopaque) Error!void {
        // Our flush is implemented by the write method, that recognizes a
        // buffer set to FLUSH with a count of zero as a flush request.
        return write(ptr, "");
    }

    fn tell(ptr: *anyopaque) Error!usize {
        const self: *FdsetReadWriter = @ptrCast(@alignCast(ptr));
        return self.pos;
    }

    pub fn deinit(self: *FdsetReadWriter) void {
        allocator.free(self.fds);
        allocator.free(self.state);
        sds.free(allocator.child, self.buf);
    }
};

/// This function can be installed both in memory and file streams when checksum
/// computation is needed.
pub fn genericUpdateChecksum(rw: *ReadWriter, buf: []const u8) void {
    rw.cksum.update(buf);
}

const std = @import("std");
const util = @import("util.zig");
const memlib = @import("mem.zig");
const memcpy = memlib.memcpy;
const sds = @import("sds.zig");
const allocator = @import("allocator.zig");
const Server = @import("Server.zig");
const posix = std.posix;
