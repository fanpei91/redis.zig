/// SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
pub fn setCommand(cli: *Client) void {
    const eqlCase = std.ascii.eqlIgnoreCase;
    var flags: u32 = OBJ_SET_NO_FLAGS;
    var unit: u32 = Server.UNIT_SECONDS;
    var expire: ?*Object = null;
    const argv = cli.argv.?;

    var j: usize = 3;
    while (j < cli.argc) : (j += 1) {
        const arg = sds.castBytes(argv[j].v.ptr);
        const next = if (j == cli.argc - 1) null else argv[j + 1];

        if (eqlCase(arg, "nx") and flags & OBJ_SET_XX == 0) {
            flags |= OBJ_SET_NX;
            continue;
        }
        if (eqlCase(arg, "xx") and flags & OBJ_SET_NX == 0) {
            flags |= OBJ_SET_XX;
            continue;
        }
        if (eqlCase(arg, "ex") and flags & OBJ_SET_PX == 0 and next != null) {
            flags |= OBJ_SET_EX;
            unit = Server.UNIT_SECONDS;
            expire = next;
            j += 1;
            continue;
        }
        if (eqlCase(arg, "px") and flags & OBJ_SET_EX == 0 and next != null) {
            flags |= OBJ_SET_PX;
            unit = Server.UNIT_MILLISECONDS;
            expire = next;
            j += 1;
            continue;
        }
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    argv[2] = argv[2].tryEncoding();
    const key = argv[1];
    const val = argv[2];
    set(cli, flags, key, val, expire, unit, null, null);
}

/// SETNX key val
/// RESP: 0 if the key was not set.
/// RESP: 1 if the key was set.
pub fn setnxCommand(cli: *Client) void {
    const argv = cli.argv.?;
    argv[2] = argv[2].tryEncoding();
    const key = argv[1];
    const val = argv[2];
    set(
        cli,
        OBJ_SET_NX,
        key,
        val,
        null,
        Server.UNIT_SECONDS,
        Server.shared.cone,
        Server.shared.czero,
    );
}

/// SETEX key seconds value
pub fn setexCommand(cli: *Client) void {
    const argv = cli.argv.?;
    argv[3] = argv[3].tryEncoding();
    const key = argv[1];
    const val = argv[3];
    const expire = argv[2];
    set(
        cli,
        OBJ_SET_NO_FLAGS,
        key,
        val,
        expire,
        Server.UNIT_SECONDS,
        null,
        null,
    );
}

/// PSETEX key milliseconds value
pub fn psetexCommand(cli: *Client) void {
    const argv = cli.argv.?;
    argv[3] = argv[3].tryEncoding();
    const key = argv[1];
    const val = argv[3];
    const expire = argv[2];
    set(
        cli,
        OBJ_SET_NO_FLAGS,
        key,
        val,
        expire,
        Server.UNIT_MILLISECONDS,
        null,
        null,
    );
}

/// MSET key value [key value ...]
pub fn msetCommand(cli: *Client) void {
    mset(cli, false);
}

/// MSETNX key value [key value ...]
pub fn msetnxCommand(cli: *Client) void {
    mset(cli, true);
}

/// GET key
pub fn getCommand(cli: *Client) void {
    _ = get(cli);
}

/// MGET key [key ...]
pub fn mgetCommand(cli: *Client) void {
    cli.addReplyMultiBulkLen(@intCast(cli.argc - 1));
    const argv = cli.argv.?;
    for (argv[1..cli.argc]) |key| {
        const val = cli.db.lookupKeyRead(key) orelse {
            cli.addReply(Server.shared.nullbulk);
            continue;
        };
        if (val.type != .string) {
            cli.addReply(Server.shared.nullbulk);
        } else {
            cli.addReplyBulk(val);
        }
    }
}

/// GETSET key value
pub fn getsetCommand(cli: *Client) void {
    if (!get(cli)) return;
    const argv = cli.argv.?;
    argv[2] = argv[2].tryEncoding();
    const key = argv[1];
    const val = argv[2];
    cli.db.setKey(key, val);
    server.dirty +%= 1;
}

/// INCR key
pub fn incrCommand(cli: *Client) void {
    incr(cli, 1);
}

/// DECR key
pub fn decrCommand(cli: *Client) void {
    incr(cli, -1);
}

/// INCRBY key increment
pub fn incrbyCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const increment = argv[2].getLongLongOrReply(cli, null) orelse return;
    incr(cli, increment);
}

/// DECRBY key decrement
pub fn decrbyCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const decrement = argv[2].getLongLongOrReply(cli, null) orelse return;
    incr(cli, -decrement);
}

/// INCRBYFLOAT key increment
pub fn incrbyfloatCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const o = cli.db.lookupKeyWrite(key);
    if (o) |v| if (v.checkTypeOrReply(cli, .string)) return;
    var value: f80 = blk: {
        if (o) |v| {
            break :blk v.getLongDoubleOrReply(cli, null) orelse 0;
        }
        break :blk 0;
    };
    const increment = argv[2].getLongDoubleOrReply(cli, null) orelse return;
    value += increment;
    if (std.math.isNan(value) or std.math.isInf(value)) {
        cli.addReplyErr("increment would produce NaN or Infinity");
        return;
    }
    const new = Object.createStringFromLongDouble(value, true);
    defer new.decrRefCount();
    if (o != null) {
        cli.db.overwrite(key, new);
    } else {
        cli.db.add(key, new);
    }
    cli.db.signalModifiedKey(key);
    server.dirty +%= 1;
    cli.addReplyBulk(new);

    // Always replicate INCRBYFLOAT as a SET command with the final value
    // in order to make sure that differences in float precision or formatting
    // will not create differences in replicas or after an AOF restart.
    cli.rewriteCommandArgument(0, Server.shared.set);
    cli.rewriteCommandArgument(2, new);
}

/// STRLEN key
pub fn strlenCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const val = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (val.checkTypeOrReply(cli, .string)) return;
    const len = val.stringLen();
    cli.addReplyLongLong(@intCast(len));
}

/// APPEND key value
pub fn appendCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];

    var totlen: usize = undefined;
    if (cli.db.lookupKeyWrite(key)) |obj| {
        if (obj.checkTypeOrReply(cli, .string)) {
            return;
        }
        const append = argv[2];
        totlen = obj.stringLen() + sds.getLen(sds.cast(append.v.ptr));
        if (!checkStringLengthOrReply(totlen, cli)) {
            return;
        }
        const o = cli.db.unshareStringValue(key, obj);
        o.v = .{
            .ptr = sds.cat(
                allocator.impl,
                sds.cast(o.v.ptr),
                sds.castBytes(append.v.ptr),
            ),
        };
        totlen = sds.getLen(sds.cast(o.v.ptr));
    } else {
        argv[2] = argv[2].tryEncoding();
        const append = argv[2];
        cli.db.add(key, append);
        totlen = append.stringLen();
    }
    cli.db.signalModifiedKey(key);
    server.dirty +%= 1;
    cli.addReplyLongLong(@intCast(totlen));
}

/// SETRANGE key offset value
pub fn setrangeCommand(cli: *Client) void {
    const argv = cli.argv.?;

    const offset = argv[2].getLongLongOrReply(cli, null) orelse return;
    if (offset < 0) {
        cli.addReplyErr("offset is out of range");
        return;
    }

    const key = argv[1];
    const value = sds.cast(argv[3].v.ptr);
    const length = @as(usize, @intCast(offset)) + sds.getLen(value);
    var o = cli.db.lookupKeyWrite(key);
    if (o == null) {
        // Return 0 when setting nothing on a non-existing string
        if (sds.getLen(value) == 0) {
            cli.addReply(Server.shared.czero);
            return;
        }

        // Return when the resulting string exceeds allowed size
        if (!checkStringLengthOrReply(length, cli)) {
            return;
        }

        o = Object.create(.string, sds.newLen(
            allocator.impl,
            sds.NOINIT.ptr,
            length,
        ));
        defer o.?.decrRefCount();
        cli.db.add(key, o.?);
    } else {
        const obj = o.?;
        if (obj.checkTypeOrReply(cli, .string)) {
            return;
        }

        // Return existing string length when setting nothing
        const olen = obj.stringLen();
        if (sds.getLen(value) == 0) {
            cli.addReplyLongLong(@intCast(olen));
            return;
        }

        if (!checkStringLengthOrReply(length, cli)) {
            return;
        }

        // Create a copy when the object is shared or encoded.
        o = cli.db.unshareStringValue(key, obj);
    }
    const obj = o.?;
    var s = sds.cast(obj.v.ptr);
    if (sds.getLen(value) > 0) {
        obj.v = .{ .ptr = sds.growZero(allocator.impl, s, length) };
        s = sds.cast(obj.v.ptr);
        memcpy(
            s + @as(usize, @intCast(offset)),
            value,
            sds.getLen(value),
        );
        cli.db.signalModifiedKey(key);
        server.dirty +%= 1;
    }
    cli.addReplyLongLong(@intCast(sds.getLen(s)));
}

/// GETRANGE key start end
pub fn getrangeCommand(cli: *Client) void {
    const argv = cli.argv.?;

    var start = argv[2].getLongLongOrReply(cli, null) orelse return;
    var end = argv[3].getLongLongOrReply(cli, null) orelse return;
    const key = argv[1];
    const obj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptybulk,
    ) orelse {
        return;
    };
    if (obj.checkTypeOrReply(cli, .string)) {
        return;
    }

    var buf: [32]u8 = undefined;
    var str: []u8 = undefined;
    if (obj.encoding == .int) {
        str = util.ll2string(&buf, obj.v.int);
    } else {
        str = sds.castBytes(obj.v.ptr);
    }

    // Convert negative indexes
    if (start < 0 and end < 0 and start > end) {
        cli.addReply(Server.shared.emptybulk);
        return;
    }
    const len: i64 = @intCast(str.len);
    if (start < 0) start = len +% start;
    if (end < 0) end = len +% end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if (end >= len) end = len - 1;

    // Precondition: end >= 0 && end < strlen, so the only condition where
    // nothing can be returned is: start > end.
    if (start > end or len == 0) {
        cli.addReply(Server.shared.emptybulk);
        return;
    }
    cli.addReplyBulkString(str[@intCast(start)..@intCast(end + 1)]);
}

const OBJ_SET_NO_FLAGS = 0;
const OBJ_SET_NX = (1 << 0); // Set if key not exists.
const OBJ_SET_XX = (1 << 1); // Set if key exists.
const OBJ_SET_EX = (1 << 2); // Set if time in seconds is given
const OBJ_SET_PX = (1 << 3); // Set if time in ms in given

/// The set() function implements the SET operation with different options and
/// variants. This function is called in order to implement the following
/// commands: SET, SETEX, PSETEX, SETNX.
///
/// 'flags' changes the behavior of the command (NX or XX, see belove).
///
/// 'expire' represents an expire to set in form of a Redis object as passed
/// by the user. It is interpreted according to the specified 'unit'.
///
/// 'ok_reply' and 'abort_reply' is what the function will reply to the client
/// if the operation is performed, or when it is not because of NX or
/// XX flags.
///
/// If ok_reply is null "+OK" is used.
/// If abort_reply is null, "$-1" is used.
fn set(
    cli: *Client,
    flags: u32,
    key: *Object,
    val: *Object,
    expire: ?*Object,
    unit: u32,
    ok_reply: ?*Object,
    abort_reply: ?*Object,
) void {
    var milliseconds: i64 = 0;
    if (expire) |exp| {
        milliseconds = exp.getLongLongOrReply(cli, null) orelse return;
        if (milliseconds <= 0) {
            cli.addReplyErrFormat(
                "invalid expire time in {s}",
                .{cli.cmd.?.name},
            );
            return;
        }
        if (unit == Server.UNIT_SECONDS) {
            milliseconds *|= std.time.ms_per_s;
        }
    }

    if (flags & OBJ_SET_NX != 0 and cli.db.lookupKeyWrite(key) != null or
        flags & OBJ_SET_XX != 0 and cli.db.lookupKeyWrite(key) == null)
    {
        cli.addReply(
            if (abort_reply) |abort|
                abort
            else
                Server.shared.nullbulk,
        );
        return;
    }

    cli.db.setKey(key, val);
    server.dirty +%= 1;
    if (expire != null) {
        cli.db.setExpire(
            cli,
            key,
            std.time.milliTimestamp() +| milliseconds,
        );
    }
    cli.addReply(
        if (ok_reply) |ok|
            ok
        else
            Server.shared.ok,
    );
}

fn incr(cli: *Client, by: i64) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const o = cli.db.lookupKeyWrite(key);
    if (o) |v| if (v.checkTypeOrReply(cli, .string)) return;

    var value: i64 = blk: {
        if (o) |v| {
            break :blk v.getLongLongOrReply(cli, null) orelse return;
        }
        break :blk 0;
    };

    value = std.math.add(i64, value, by) catch {
        cli.addReplyErr("increment or decrement would overflow");
        return;
    };

    var new: *Object = undefined;
    if ((o != null) and
        (o.?.refcount == 1 and o.?.encoding == .int) and
        (value < 0 or value >= Server.OBJ_SHARED_INTEGERS))
    {
        new = o.?;
        new.v = .{ .int = value };
    } else {
        new = Object.createStringFromLonglongForValue(value);
        defer new.decrRefCount();
        if (o != null) {
            cli.db.overwrite(key, new);
        } else {
            cli.db.add(key, new);
        }
    }

    cli.db.signalModifiedKey(key);
    server.dirty +%= 1;

    cli.addReply(Server.shared.colon);
    cli.addReply(new);
    cli.addReply(Server.shared.crlf);
}

fn mset(cli: *Client, nx: bool) void {
    if (cli.argc % 2 == 0) {
        cli.addReplyErr("wrong number of arguments for MSET");
        return;
    }

    const argv = cli.argv.?;
    // Handle the NX flag. The MSETNX semantic is to return zero and don't
    // set anything if at least one key alerady exists.
    if (nx) {
        var j: usize = 1;
        while (j < cli.argc) : (j += 2) {
            const key = argv[j];
            if (cli.db.lookupKeyWrite(key) != null) {
                cli.addReply(Server.shared.czero);
                return;
            }
        }
    }

    var j: usize = 1;
    while (j < cli.argc) : (j += 2) {
        argv[j + 1] = argv[j + 1].tryEncoding();
        const key = argv[j];
        const val = argv[j + 1];
        cli.db.setKey(key, val);
    }
    server.dirty +%= @intCast(@divExact(cli.argc - 1, 2));
    cli.addReply(if (nx) Server.shared.cone else Server.shared.ok);
}

fn get(cli: *Client) bool {
    const argv = cli.argv.?;
    const key = argv[1];
    const val = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return true;
    };
    if (val.type != .string) {
        cli.addReply(Server.shared.wrongtypeerr);
        return false;
    }
    cli.addReplyBulk(val);
    return true;
}

fn checkStringLengthOrReply(size: usize, cli: *Client) bool {
    if (size > 512 * 1024 * 1024) {
        cli.addReplyErr("string exceeds maximum allowed size (512MB)");
        return false;
    }
    return true;
}

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const util = @import("util.zig");
const allocator = @import("allocator.zig");
const server = &Server.instance;
