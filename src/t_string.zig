/// SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
pub fn setCommand(cli: *Client) void {
    const eqlCase = std.ascii.eqlIgnoreCase;
    var flags: u32 = OBJ_SET_NO_FLAGS;
    var unit: u32 = Server.UNIT_SECONDS;
    var expire: ?*Object = null;
    const argv = cli.argv.?;

    var j: usize = 3;
    while (j < cli.argc) : (j += 1) {
        const arg = sds.asBytes(sds.cast(argv[j].data.ptr));
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

/// GETSET key value
pub fn getsetCommand(cli: *Client) void {
    if (!get(cli)) return;
    const argv = cli.argv.?;
    argv[2] = argv[2].tryEncoding();
    const key = argv[1];
    const val = argv[2];
    cli.db.setKey(key, val);
}

/// GET key
pub fn getCommand(cli: *Client) void {
    _ = get(cli);
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
    var increment: i64 = undefined;
    if (!cli.argv.?[2].getLongLongOrReply(cli, &increment, null)) {
        return;
    }
    incr(cli, increment);
}

/// DECRBY key decrement
pub fn decrbyCommand(cli: *Client) void {
    var decrement: i64 = undefined;
    if (!cli.argv.?[2].getLongLongOrReply(cli, &decrement, null)) {
        return;
    }
    incr(cli, -decrement);
}

/// STRLEN key
pub fn strlenCommand(cli: *Client) void {
    const key = cli.argv.?[1];
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
        totlen = obj.stringLen() + sds.getLen(sds.cast(append.data.ptr));
        if (!checkStringLengthOrReply(totlen, cli)) {
            return;
        }
        const o = cli.db.unshareStringValue(key, obj);
        o.data = .{
            .ptr = sds.cat(
                sds.cast(o.data.ptr),
                sds.asBytes(sds.cast(append.data.ptr)),
            ),
        };
        totlen = sds.getLen(sds.cast(o.data.ptr));
    } else {
        argv[2] = argv[2].tryEncoding();
        const append = argv[2];
        cli.db.add(key, append);
        append.incrRefCount();
        totlen = append.stringLen();
    }
    cli.addReplyLongLong(@intCast(totlen));
}

fn incr(cli: *Client, by: i64) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const o = cli.db.lookupKeyWrite(key);
    if (o != null and o.?.checkTypeOrReply(cli, .string)) return;

    var value: i64 = 0;
    if (o != null and !o.?.getLongLongOrReply(cli, &value, null)) return;

    if ((by < 0 and value < 0 and by < std.math.minInt(i64) - value) or
        (by > 0 and value > 0 and by > std.math.maxInt(i64) - value))
    {
        cli.addReplyErr("increment or decrement would overflow");
        return;
    }

    value += by;

    var new: *Object = undefined;
    if ((o != null) and
        (o.?.refcount == 1 and o.?.encoding == .int) and
        (value < 0 or value >= Server.OBJ_SHARED_INTEGERS))
    {
        new = o.?;
        new.data = .{ .int = value };
    } else {
        new = Object.createStringFromLonglongForValue(value);
        if (o != null) {
            cli.db.overwrite(key, new);
        } else {
            cli.db.add(key, new);
        }
    }

    cli.addReply(Server.shared.colon);
    cli.addReply(new);
    cli.addReply(Server.shared.crlf);
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
        if (!exp.getLongLongOrReply(cli, &milliseconds, null)) {
            return;
        }
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

fn get(cli: *Client) bool {
    const key = cli.argv.?[1];
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
