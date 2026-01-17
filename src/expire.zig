/// TTL key
pub fn ttlCommand(cli: *Client) void {
    ttl(cli, false);
}

/// PTTL key
pub fn pttlCommand(cli: *Client) void {
    ttl(cli, true);
}

/// PERSIST key
pub fn persistCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    if (cli.db.lookupKeyWrite(key) != null) {
        if (cli.db.removeExpire(key)) {
            cli.addReply(Server.shared.cone);
            server.dirty +%= 1;
        } else {
            cli.addReply(Server.shared.czero);
        }
    } else {
        cli.addReply(Server.shared.czero);
    }
}

/// TOUCH key [key ...]
pub fn touchCommand(cli: *Client) void {
    const argv = cli.argv.?;
    var touched: i64 = 0;
    for (argv[1..]) |key| {
        if (cli.db.lookupKeyRead(key) != null) {
            touched += 1;
        }
    }
    cli.addReplyLongLong(touched);
}

/// EXPIRE key seconds
pub fn expireCommand(cli: *Client) void {
    expireAt(cli, std.time.milliTimestamp(), Server.UNIT_SECONDS);
}

/// EXPIREAT key sec_time
pub fn expireatCommand(cli: *Client) void {
    expireAt(cli, 0, Server.UNIT_SECONDS);
}

/// PEXPIRE key milliseconds
pub fn pexpireCommand(cli: *Client) void {
    expireAt(cli, std.time.milliTimestamp(), Server.UNIT_MILLISECONDS);
}

/// PEXPIREAT key ms_time
pub fn pexpireatCommand(cli: *Client) void {
    expireAt(cli, 0, Server.UNIT_MILLISECONDS);
}

/// This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
/// and PEXPIREAT. Because the commad second argument may be relative or absolute
/// the "basetime" argument is used to signal what the base time is (either 0
/// for *AT variants of the command, or the current time for relative expires).
///
/// unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
/// the argv[2] parameter. The basetime is always specified in milliseconds.
fn expireAt(cli: *Client, basetime: i64, unit: u32) void {
    const argv = cli.argv.?;

    // unix time in milliseconds when the key will expire.
    var when = argv[2].getLongLongOrReply(cli, null) orelse return;

    if (unit == Server.UNIT_SECONDS) {
        when *= std.time.ms_per_s;
    }
    when += basetime;

    // No key, return zero.
    const key = argv[1];
    if (cli.db.lookupKeyWrite(key) == null) {
        cli.addReply(Server.shared.czero);
        return;
    }

    if (checkAlreadyExpired(when)) {
        assert(cli.db.delete(key));
        server.dirty +%= 1;

        //Replicate/AOF this as an explicit DEL or UNLINK.
        const cmd = if (server.lazyfree_lazy_expire)
            Server.shared.unlink
        else
            Server.shared.del;
        cli.rewriteCommandVector(&.{ cmd, key });
        cli.db.signalModifiedKey(key);
        cli.addReply(Server.shared.cone);
        return;
    }
    cli.db.setExpire(cli, key, when);
    cli.db.signalModifiedKey(key);
    cli.addReply(Server.shared.cone);
    server.dirty +%= 1;
}

fn checkAlreadyExpired(when: i64) bool {
    // EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
    // should never be executed as a DEL when load the AOF or in the context
    // of a slave instance.
    //
    // Instead we add the already expired key to the database with expire time
    // (possibly in the past) and wait for an explicit DEL from the master.
    return std.time.milliTimestamp() >= when and !server.loading;
}

/// Implements TTL and PTTL
fn ttl(cli: *Client, output_ms: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // If the key does not exist at all, return -2
    _ = cli.db.lookupKeyReadWithFlags(key, Server.LOOKUP_NOTOUCH) orelse {
        cli.addReplyLongLong(-2);
        return;
    };

    // The key exists. Return -1 if it has no expire, or the actual
    // TTL value otherwise.
    var t: i64 = -1;
    const expire = cli.db.getExpire(key);
    if (expire != -1) {
        t = expire - std.time.milliTimestamp();
        if (t < 0) t = 0;
    }
    if (t == -1) {
        cli.addReplyLongLong(-1);
    } else {
        cli.addReplyLongLong(
            if (output_ms)
                t
            else
                @divFloor(t, std.time.ms_per_s),
        );
    }
}

/// Try to expire a few timed out keys. The algorithm used is adaptive and
/// will use few CPU cycles if there are few expiring keys, otherwise
/// it will get more aggressive to avoid that too much memory is used by
/// keys that can be removed from the keyspace.
///
/// No more than CRON_DBS_PER_CALL databases are tested at every
/// iteration.
///
/// This kind of call is used when Redis detects that timelimit_exit is
/// true, so there is more work to do, and we do it more incrementally from
/// the beforeSleep() function of the event loop.
///
/// Expire cycle type:
///
/// If type is ACTIVE_EXPIRE_CYCLE_FAST the function will try to run a
/// "fast" expire cycle that takes no longer than EXPIRE_FAST_CYCLE_DURATION
/// microseconds, and is not repeated again before the same amount of time.
///
/// If type is ACTIVE_EXPIRE_CYCLE_SLOW, that normal expire cycle is
/// executed, where the time limit is a percentage of the REDIS_HZ period
/// as specified by the ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC define.
pub fn activeExpireCycle(@"type": i32) void {
    // This function has some global state in order to continue the work
    // incrementally across calls.
    const State = struct {
        /// Last DB tested.
        var current_db: usize = 0;
        /// Time limit hit in previous call?
        var timelimit_exit: bool = false;
        ///  When last fast cycle ran.
        var last_fast_cycle: i64 = 0;
    };

    var dbs_per_call: usize = Server.CRON_DBS_PER_CALL;
    const start = std.time.microTimestamp();

    if (@"type" == Server.ACTIVE_EXPIRE_CYCLE_FAST) {
        // Don't start a fast cycle if the previous cycle did not exit
        // for time limit. Also don't repeat a fast cycle for the same period
        // as the fast cycle total duration itself.
        if (!State.timelimit_exit) return;
        if (start < State.last_fast_cycle +
            Server.ACTIVE_EXPIRE_CYCLE_FAST_DURATION * 2) return;
        State.last_fast_cycle = start;
    }

    // We usually should test CRON_DBS_PER_CALL per iteration, with
    // two exceptions:
    //
    // 1) Don't test more DBs than we have.
    // 2) If last time we hit the time limit, we want to scan all DBs
    // in this iteration, as there is work to do in some DB and we don't want
    // expired keys to use memory for too much time.
    if (dbs_per_call > server.dbnum or State.timelimit_exit) {
        dbs_per_call = server.dbnum;
    }

    // We can use at max ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC percentage of CPU time
    // per iteration. Since this function gets called with a frequency of
    // server.hz times per second, the following is the max amount of
    // microseconds we can spend in this function.
    var timelimit: i64 = @divFloor(
        @as(i64, 1000000) * Server.ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC,
        @as(i64, @intCast(server.hz * 100)),
    );
    if (timelimit <= 0) timelimit = 1;
    if (@"type" == Server.ACTIVE_EXPIRE_CYCLE_FAST) {
        timelimit = Server.ACTIVE_EXPIRE_CYCLE_FAST_DURATION;
    }

    var iteration: usize = 0;
    var elapsed: i64 = undefined;

    var j: usize = 0;
    while (j < dbs_per_call and !State.timelimit_exit) : (j += 1) {
        const db = &server.db[@rem(State.current_db, server.dbnum)];

        // Increment the DB now so we are sure if we run out of time
        // in the current DB we'll restart from the next. This allows to
        // distribute the time evenly across DBs.
        State.current_db +%= 1;

        // Continue to expire if at the end of the cycle more than 25%
        // of the keys were expired.
        while (true) {
            iteration += 1;

            // If there is nothing to expire try next DB ASAP.
            var num = db.expires.size();
            if (num == 0) break;

            const slots = db.expires.slots();
            const now = std.time.milliTimestamp();

            // When there are less than 1% filled slots getting random
            // keys is expensive, so stop here waiting for better times...
            // The dictionary will be resized asap.
            if (num > 0 and slots > dict.HT_INITIAL_SIZE and
                @divFloor(num * 100, slots) < 1)
            {
                break;
            }

            // The main collection cycle. Sample random keys among keys
            // with an expire set, checking for expired ones.
            var expired: usize = 0;
            num = @min(num, Server.ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP);
            while (num != 0) : (num -= 1) {
                const de = db.expires.getRandom() orelse break;
                if (activeExpireCycleTryExpire(db, de, now)) expired += 1;
            }

            // We can't block forever here even if there are many keys to
            // expire. So after a given amount of milliseconds return to the
            // caller waiting for the other active expire cycle.
            if ((iteration & 0xf) == 0) { // check once every 16 iterations.
                elapsed = std.time.microTimestamp() - start;
                if (elapsed > timelimit) {
                    State.timelimit_exit = true;
                    break;
                }
            }

            // We don't repeat the cycle if there are less than 25% of keys
            // found expired in the current DB.
            if (expired > @divFloor(
                Server.ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP,
                4,
            )) {
                break;
            }
        }
    }
}

/// Helper function for the activeExpireCycle() function.
/// This function will try to expire the key that is stored in the hash table
/// entry 'de' of the 'expires' hash table of a Redis database.
///
/// If the key is found to be expired, it is removed from the database and
/// TRUE is returned. Otherwise no operation is performed and FALSE is returned.
///
/// When a key is expired, server.stat_expiredkeys is incremented.
///
/// The parameter 'now' is the current time in milliseconds as is passed
/// to the function to avoid too many gettimeofday() syscalls.
fn activeExpireCycleTryExpire(
    db: *Database,
    de: *Database.Expires.Entry,
    now: i64,
) bool {
    const t = de.val;
    if (now > t) {
        const key = de.key;
        const keyobj = Object.createString(sds.asBytes(key));
        defer keyobj.decrRefCount();
        db.propagateExpire(keyobj, server.lazyfree_lazy_expire);
        if (server.lazyfree_lazy_expire) {
            _ = lazyfree.asyncDelete(db, keyobj);
        } else {
            _ = db.syncDelete(keyobj);
        }
        return true;
    }
    return false;
}

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Object = @import("Object.zig");
const server = &Server.instance;
const assert = std.debug.assert;
const networking = @import("networking.zig");
const dict = @import("dict.zig");
const Database = @import("db.zig").Database;
const sds = @import("sds.zig");
const lazyfree = @import("lazyfree.zig");
