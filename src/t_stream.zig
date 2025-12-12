/// XADD key [MAXLEN [~|=] <count>] <ID or *> [field value] [field value] ...
pub fn xaddCommand(cli: *CLient) void {
    const argv = cli.argv.?;

    // Was an ID different than "*" specified?
    var id_given = false;
    // If left to -1 no trimming is performed.
    var maxlen: i64 = -1;
    // If true only delete whole radix tree nodes, so the maxium length is not
    // applied verbatim.
    var approx_maxlen: bool = false;

    // Parse options.
    //
    // This is the first argument position where we could
    // find an option, or the ID.
    var i: usize = 2;
    var id: Stream.Id = undefined;
    while (i < cli.argc) : (i += 1) {
        // Number of additional arguments.
        const moreargs = cli.argc - 1 - i;
        const opt = sds.castBytes(argv[i].v.ptr);
        if (opt[0] == '*' and opt.len == 1) {
            // This is just a fast path for the common case of auto-ID
            // creation.
            break;
        } else if (eqlCase(opt, "maxlen") and moreargs > 0) {
            approx_maxlen = false;
            const next = sds.castBytes(argv[i + 1].v.ptr);
            // Check for the form MAXLEN ~ <count>.
            if (moreargs >= 2 and next[0] == '~' and next.len == 1) {
                approx_maxlen = true;
                i += 1;
            } else if (moreargs >= 2 and next[0] == '=' and next.len == 1) {
                i += 1;
            }

            maxlen = argv[i + 1].getLongLongOrReply(cli, null) orelse {
                return;
            };
            if (maxlen < 0) {
                cli.addReplyErr("The MAXLEN argument must be >= 0.");
                return;
            }
            i += 1;
        } else {
            // If we are here is a syntax error or a valid ID.
            if (!parseIdOrReply(cli, argv[i], &id, 0, true)) return;
            id_given = true;
            break;
        }
    }
    const field_pos = i + 1;

    // Check arity.
    if ((cli.argc - field_pos) < 2 or ((cli.argc - field_pos) % 2) == 1) {
        cli.addReplyErr("wrong number of arguments for XADD");
        return;
    }

    // Return ASAP if minimal ID (0-0) was given so we avoid possibly creating
    // a new stream and have streamAppendItem fail, leaving an empty key in the
    // database.
    if (id_given and id.ms == 0 and id.seq == 0) {
        cli.addReplyErr("The ID specified in XADD must be greater than 0-0");
        return;
    }

    const key = argv[1];
    const xobj = lookupCreateOrReply(cli, key) orelse {
        return;
    };
    const stream: *Stream = .cast(xobj.v.ptr);

    // Return ASAP if the stream has reached the last possible ID
    const last_id = stream.last_id;
    if (last_id.ms == maxInt(u64) and last_id.seq == maxInt(u64)) {
        cli.addReplyErr("The stream has exhausted the last possible ID, " ++
            "unable to add more items");
        return;
    }

    const fields = argv[field_pos..];
    stream.append(
        fields,
        @divExact(fields.len, 2),
        &id,
        if (id_given) &id else null,
    ) catch |err| {
        switch (err) {
            error.DOM => cli.addReplyErr("The ID specified in XADD is equal or " ++
                "smaller than the target stream top item"),
            error.RANGE => cli.addReplyErr("Elements are too large to be stored"),
        }
        return;
    };

    cli.addReplyStreamID(&id);

    if (maxlen >= 0) {
        _ = stream.trimByLength(@intCast(maxlen), approx_maxlen);
    }
}

/// Return the length of a stream.
pub fn length(o: *const Object) u64 {
    const s: *Stream = .cast(o.v.ptr);
    return s.length;
}

/// Look the stream at 'key' and return the corresponding stream object.
/// The function creates a key setting it to an empty stream if needed.
fn lookupCreateOrReply(cli: *CLient, key: *Object) ?*Object {
    const o = cli.db.lookupKeyWrite(key) orelse {
        const o = Object.createStream();
        defer o.decrRefCount();
        cli.db.add(key, o);
        return o;
    };
    if (o.checkTypeOrReply(cli, .stream)) {
        return null;
    }
    return o;
}

/// Parse a stream ID in the format given by clients to Redis, that is
/// <ms>-<seq>, and converts it into a Stream.Id structure. If
/// the specified ID is invalid FALSE is returned and an error is reported
/// to the client, otherwise TRUE is returned. The ID may be in incomplete
/// form, just stating the milliseconds time part of the stream. In such a case
/// the missing part is set according to the value of 'missing_seq' parameter.
///
/// The IDs "-" and "+" specify respectively the minimum and maximum IDs
/// that can be represented. If 'strict' is set to TRUE, "-" and "+" will be
/// treated as an invalid ID.
///
/// If 'cli' is set to NULL, no reply is sent to the client.
fn parseIdOrReply(
    cli: ?*CLient,
    arg: *Object,
    id: *Stream.Id,
    missing_seq: u64,
    strict: bool,
) bool {
    biz: {
        const o = sds.castBytes(arg.v.ptr);
        if (o.len > 128) break :biz;
        if (strict and o.len == 1 and (o[0] == '-' or o[0] == '+')) {
            break :biz;
        }
        // Handle the "-" and "+" special cases.
        if (o.len == 1 and o[0] == '-') {
            id.ms = 0;
            id.seq = 0;
            return true;
        } else if (o.len == 1 and o[0] == '+') {
            id.ms = maxInt(u64);
            id.seq = maxInt(u64);
            return true;
        }
        // Parse <ms>-<seq> form.
        const dot = std.mem.indexOf(u8, o, "-");
        id.ms = util.string2ull(o[0 .. dot orelse o.len]) orelse break :biz;
        if (dot) |i| {
            id.seq = util.string2ull(o[i + 1 ..]) orelse break :biz;
        } else {
            id.seq = missing_seq;
        }
        return true;
    }
    if (cli) |c| {
        c.addReplyErr("Invalid stream ID specified as stream command argument");
    }
    return false;
}

const std = @import("std");
const Stream = @import("Stream.zig");
const CLient = @import("networking.zig").Client;
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const eqlCase = std.ascii.eqlIgnoreCase;
const memlib = @import("mem.zig");
const memcpy = memlib.memcpy;
const maxInt = std.math.maxInt;
const util = @import("util.zig");
const Server = @import("Server.zig");
const server = &Server.instance;
const blocked = @import("blocked.zig");
const raxlib = @import("rax/rax.zig").rax;
const ListPack = @import("ListPack.zig");
