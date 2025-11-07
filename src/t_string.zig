/// SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
pub fn setCommand(cli: *Client) Allocator.Error!void {
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
        try cli.addReply(Server.shared.syntaxerr);
        return;
    }

    argv[2] = try argv[2].tryEncoding(cli.allocator);
    const key = argv[1];
    const val = argv[2];
    try set(cli, flags, key, val, expire, unit, null, null);
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
) Allocator.Error!void {
    var milliseconds: i64 = 0;
    if (expire) |exp| {
        if (!try exp.getLongLongOrReply(cli, &milliseconds, null)) {
            return;
        }
        if (milliseconds <= 0) {
            try cli.addReplyErrFormat(
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
        try cli.addReply(
            if (abort_reply) |abort|
                abort
            else
                Server.shared.nullbulk,
        );
        return;
    }

    try cli.db.setKey(key, val);
    if (expire != null) {
        try cli.db.setExpire(
            cli,
            key,
            std.time.milliTimestamp() +| milliseconds,
        );
    }
    try cli.addReply(
        if (ok_reply) |ok|
            ok
        else
            Server.shared.ok,
    );
}

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Allocator = std.mem.Allocator;
const Object = @import("Object.zig");
const server = &Server.instance;
const sds = @import("sds.zig");
