/// MULTI
pub fn multiCommand(cli: *Client) void {
    if (cli.flags & Server.CLIENT_MULTI != 0) {
        cli.addReplyErr("MULTI calls can not be nested");
        return;
    }
    cli.flags |= Server.CLIENT_MULTI;
    cli.addReply(Server.shared.ok);
}

/// EXEC
pub fn execCommand(cli: *Client) void {
    if (cli.flags & Server.CLIENT_MULTI == 0) {
        cli.addReplyErr("EXEC without MULTI");
        return;
    }
    biz: {
        // Check if we need to abort the EXEC because:
        // 1) Some WATCHed key was touched.
        // 2) There was a previous error while queueing commands.
        // A failed EXEC in the first case returns a multi bulk nil object
        // (technically it is not an error but a special behavior), while
        // in the second an EXECABORT error is returned.
        if (cli.flags & (Server.CLIENT_DIRTY_CAS | Server.CLIENT_DIRTY_EXEC) != 0) {
            cli.addReply(
                if (cli.flags & Server.CLIENT_DIRTY_EXEC != 0)
                    Server.shared.execaborterr
                else
                    Server.shared.nullmultibulk,
            );
            discardTransaction(cli);
            break :biz;
        }

        // Exec all the queued commands
        var must_propagate = false; // Need to propagate MULTI/EXEC to AOF / slaves?
        unwatchAllKeys(cli); // Unwatch ASAP otherwise we'll waste CPU cycles
        const orig_argv = cli.argv;
        const orig_argc = cli.argc;
        const orig_cmd = cli.cmd;
        cli.addReplyMultiBulkLen(
            if (cli.mstate.commands) |commands|
                @intCast(commands.len)
            else
                0,
        );
        if (cli.mstate.commands) |commands| for (commands) |*cmd| {
            cli.argv = cmd.argv;
            cli.argc = cmd.argc;
            cli.cmd = cmd.cmd;

            // Propagate a MULTI request once we encounter the first command which
            // is not readonly nor an administrative one.
            // This way we'll deliver the MULTI/..../EXEC block as a whole and
            // both the AOF and the replication link will have the same consistency
            // and atomicity guarantees.
            if (!must_propagate and
                (cli.cmd.?.flags & (Server.CMD_READONLY | Server.CMD_ADMIN)) == 0)
            {
                execCommandPropagateMulti(cli);
                must_propagate = true;
            }

            server.call(
                cli,
                if (server.loading)
                    Server.CMD_CALL_NONE
                else
                    Server.CMD_CALL_FULL,
            );

            // Commands may alter argc/argv, restore mstate.
            cmd.argv = cli.argv.?;
            cmd.argc = cli.argc;
            cmd.cmd = cli.cmd.?;
        };

        cli.argv = orig_argv;
        cli.argc = orig_argc;
        cli.cmd = orig_cmd;
        discardTransaction(cli);

        // Make sure the EXEC command will be propagated as well if MULTI
        // was already propagated.
        if (must_propagate) {
            server.dirty +%= 1;
        }
    }
}

/// DISCARD
pub fn discardCommand(cli: *Client) void {
    if (cli.flags & Server.CLIENT_MULTI == 0) {
        cli.addReplyErr("DISCARD without MULTI");
        return;
    }
    discardTransaction(cli);
    cli.addReply(Server.shared.ok);
}

/// WATCH key [key ...]
pub fn watchCommand(cli: *Client) void {
    const argv = cli.argv.?;
    if (cli.flags & Server.CLIENT_MULTI != 0) {
        cli.addReplyErr("WATCH inside MULTI is not allowed");
        return;
    }
    for (argv[1..cli.argc]) |key| {
        watchForKey(cli, key);
    }
    cli.addReply(Server.shared.ok);
}

/// UNWATCH
pub fn unwatchCommand(cli: *Client) void {
    unwatchAllKeys(cli);
    cli.flags &= ~@as(i32, Server.CLIENT_DIRTY_CAS);
    cli.addReply(Server.shared.ok);
}

/// Flag the transacation as DIRTY_EXEC so that EXEC will fail.
/// Should be called every time there is an error while queueing a command.
pub fn flagTransaction(cli: *Client) void {
    if (cli.flags & Server.CLIENT_MULTI != 0) {
        cli.flags |= Server.CLIENT_DIRTY_EXEC;
    }
}

pub fn discardTransaction(cli: *Client) void {
    cli.mstate.deinit();
    cli.mstate.init();
    cli.flags &= ~@as(
        i32,
        Server.CLIENT_MULTI | Server.CLIENT_DIRTY_CAS | Server.CLIENT_DIRTY_EXEC,
    );
    unwatchAllKeys(cli);
}

/// Unwatch all the keys watched by this client. To clean the EXEC dirty
/// flag is up to the caller.
pub fn unwatchAllKeys(cli: *Client) void {
    if (cli.watched_keys.len == 0) {
        return;
    }
    var li = cli.watched_keys.iterator(.forward);
    while (li.next()) |ln| {
        const wk = ln.value;
        const clients = wk.db.watched_keys.fetchValue(wk.key).?;
        clients.removeNode(clients.search(cli).?);
        if (clients.len == 0) {
            assert(wk.db.watched_keys.delete(wk.key));
        }
        cli.watched_keys.removeNode(ln);
    }
}

/// "Touch" a key, so that if this key is being WATCHed by some client the
/// next EXEC will fail.
pub fn touchWatchedKey(db: *Database, key: *Object) void {
    if (db.watched_keys.size() == 0) {
        return;
    }
    const clients = db.watched_keys.fetchValue(key) orelse {
        return;
    };

    // Mark all the clients watching this key as CLIENT_DIRTY_CAS.
    // Check if we are already watching for this key
    var li = clients.iterator(.forward);
    while (li.next()) |ln| {
        const cli = ln.value;
        cli.flags |= Server.CLIENT_DIRTY_CAS;
    }
}

/// Add a new command into the MULTI commands queue
pub fn queueMultiCommand(cli: *Client) void {
    if (cli.mstate.commands) |commands| {
        cli.mstate.commands = allocator.realloc(commands, commands.len + 1);
    } else {
        cli.mstate.commands = allocator.alloc(State.Cmd, 1);
    }
    const commands = cli.mstate.commands.?;
    var cmd = &commands[commands.len - 1];
    cmd.init(cli.argv.?, cli.argc, cli.cmd.?);
}

/// Watch for the specified key
fn watchForKey(cli: *Client, key: *Object) void {
    // Check if we are already watching for this key
    var li = cli.watched_keys.iterator(.forward);
    while (li.next()) |ln| {
        const wk = ln.value;
        if (wk.db == cli.db and wk.key.equalStrings(key)) {
            // Key already watched
            return;
        }
    }
    // This key is not already watched in this DB. Let's add it.
    const clients = cli.db.watched_keys.fetchValue(key) orelse blk: {
        const clients = Server.ClientList.create(&.{});
        assert(cli.db.watched_keys.add(key, clients));
        break :blk clients;
    };
    clients.append(cli);
    // Add the new key to the list of keys watched by this client.
    const wk = WatchedKey.create(cli.db, key);
    cli.watched_keys.append(wk);
}

/// Send a MULTI command to all the slaves and AOF file. Check the execCommand
/// implementation for more information.
pub fn execCommandPropagateMulti(cli: *Client) void {
    var argv: [1]*Object = .{
        Server.shared.multi.incrRefCount(),
    };
    defer argv[0].decrRefCount();
    server.propagate(
        server.multiCommand,
        cli.db.id,
        &argv,
        argv.len,
        Server.PROPAGATE_AOF | Server.PROPAGATE_REPL,
    );
}

pub const WatchedKey = struct {
    db: *Database,
    key: *Object,

    pub fn create(db: *Database, key: *Object) *WatchedKey {
        const wk = allocator.create(WatchedKey);
        wk.db = db;
        wk.key = key;
        _ = wk.key.incrRefCount();
        return wk;
    }

    pub fn destroy(self: *WatchedKey) void {
        self.key.decrRefCount();
        allocator.destroy(self);
    }
};

pub const State = struct {
    pub const Cmd = struct {
        argv: []*Object,
        argc: usize,
        cmd: *const Server.Command,

        fn init(
            self: *Cmd,
            argv: []const *Object,
            argc: usize,
            cmd: *const Server.Command,
        ) void {
            self.argv = allocator.alloc(*Object, argc);
            self.argc = argc;
            @memcpy(self.argv, argv[0..argc]);
            for (self.argv) |arg| _ = arg.incrRefCount();
            self.cmd = cmd;
        }

        fn deinit(self: *Cmd) void {
            for (self.argv[0..self.argc]) |arg| arg.decrRefCount();
            allocator.free(self.argv);
        }
    };

    /// Array of MULTI commands
    commands: ?[]Cmd,

    /// State initialization for MULTI/EXEC
    pub fn init(self: *State) void {
        self.commands = null;
    }

    /// Release all the resources associated with MULTI/EXEC state.
    pub fn deinit(self: *State) void {
        if (self.commands) |commands| {
            for (commands) |*cmd| {
                cmd.deinit();
            }
            allocator.free(commands);
        }
    }
};

const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const server = &Server.instance;
const Object = @import("Object.zig");
const Database = @import("db.zig").Database;
const allocator = @import("allocator.zig");
const std = @import("std");
const assert = std.debug.assert;
