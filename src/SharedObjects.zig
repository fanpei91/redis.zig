const SharedObjects = @This();
crlf: *Object,
ok: *Object,
err: *Object,
emptybulk: *Object,
czero: *Object,
cone: *Object,
cnegone: *Object,
nullbulk: *Object,
nullmultibulk: *Object,
emptymultibulk: *Object,
pong: *Object,
queued: *Object,
emptyscan: *Object,
wrongtypeerr: *Object,
nokeyerr: *Object,
syntaxerr: *Object,
sameobjecterr: *Object,
outofrangeerr: *Object,
noautherr: *Object,
oomerr: *Object,
execaborterr: *Object,
busykeyerr: *Object,
space: *Object,
colon: *Object,
plus: *Object,
integers: [Server.OBJ_SHARED_INTEGERS]*Object,
bulkhdr: [Server.OBJ_SHARED_BULKHDR_LEN]*Object, // $<value>\r\n
mbulkhdr: [Server.OBJ_SHARED_BULKHDR_LEN]*Object, // *<value>\r\n
minstring: sds.String,
maxstring: sds.String,

pub fn create() SharedObjects {
    var self: SharedObjects = undefined;
    self.crlf = Object.create(
        .string,
        sds.new("\r\n"),
    );
    self.ok = Object.create(
        .string,
        sds.new("+OK\r\n"),
    );
    self.err = Object.create(
        .string,
        sds.new("-ERR\r\n"),
    );
    self.emptybulk = Object.create(
        .string,
        sds.new("$0\r\n\r\n"),
    );
    self.czero = Object.create(
        .string,
        sds.new(":0\r\n"),
    );
    self.cone = Object.create(
        .string,
        sds.new(":1\r\n"),
    );
    self.cnegone = Object.create(
        .string,
        sds.new(":-1\r\n"),
    );
    self.nullbulk = Object.create(
        .string,
        sds.new("$-1\r\n"),
    );
    self.nullmultibulk = Object.create(
        .string,
        sds.new("*-1\r\n"),
    );
    self.emptymultibulk = Object.create(
        .string,
        sds.new("*0\r\n"),
    );
    self.pong = Object.create(
        .string,
        sds.new("+PONG\r\n"),
    );
    self.queued = Object.create(
        .string,
        sds.new("+QUEUED\r\n"),
    );
    self.emptyscan = Object.create(
        .string,
        sds.new("*2\r\n$1\r\n0\r\n*0\r\n"),
    );
    self.wrongtypeerr = Object.create(
        .string,
        sds.new(
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
        ),
    );
    self.nokeyerr = Object.create(
        .string,
        sds.new("-ERR no such key\r\n"),
    );
    self.syntaxerr = Object.create(
        .string,
        sds.new("-ERR syntax error\r\n"),
    );
    self.sameobjecterr = Object.create(
        .string,
        sds.new(
            "-ERR source and destination objects are the same\r\n",
        ),
    );
    self.outofrangeerr = Object.create(
        .string,
        sds.new("-ERR index out of range\r\n"),
    );
    self.noautherr = Object.create(
        .string,
        sds.new("-NOAUTH Authentication required.\r\n"),
    );
    self.oomerr = Object.create(
        .string,
        sds.new(
            "-OOM command not allowed when used memory > 'maxmemory'.\r\n",
        ),
    );
    self.execaborterr = Object.create(
        .string,
        sds.new(
            "-EXECABORT Transaction discarded because of previous errors.\r\n",
        ),
    );
    self.busykeyerr = Object.create(
        .string,
        sds.new("-BUSYKEY Target key name already exists.\r\n"),
    );
    self.space = Object.create(
        .string,
        sds.new(" "),
    );
    self.colon = Object.create(
        .string,
        sds.new(":"),
    );
    self.plus = Object.create(
        .string,
        sds.new("+"),
    );
    for (0..Server.OBJ_SHARED_INTEGERS) |i| {
        var obj = Object.createInt(@intCast(i));
        self.integers[i] = obj.makeShared();
    }
    for (0..Server.OBJ_SHARED_BULKHDR_LEN) |i| {
        self.bulkhdr[i] = Object.create(
            .string,
            sds.catPrintf(sds.empty(), "${}\r\n", .{i}),
        );
        self.mbulkhdr[i] = Object.create(
            .string,
            sds.catPrintf(sds.empty(), "*{}\r\n", .{i}),
        );
    }
    self.minstring = sds.new("minstring");
    self.maxstring = sds.new("maxstring");
    return self;
}

pub fn destroy(self: *SharedObjects) void {
    self.crlf.decrRefCount();
    self.ok.decrRefCount();
    self.err.decrRefCount();
    self.emptybulk.decrRefCount();
    self.czero.decrRefCount();
    self.cone.decrRefCount();
    self.cnegone.decrRefCount();
    self.nullbulk.decrRefCount();
    self.nullmultibulk.decrRefCount();
    self.emptymultibulk.decrRefCount();
    self.pong.decrRefCount();
    self.queued.decrRefCount();
    self.emptyscan.decrRefCount();
    self.wrongtypeerr.decrRefCount();
    self.nokeyerr.decrRefCount();
    self.syntaxerr.decrRefCount();
    self.sameobjecterr.decrRefCount();
    self.outofrangeerr.decrRefCount();
    self.noautherr.decrRefCount();
    self.oomerr.decrRefCount();
    self.execaborterr.decrRefCount();
    self.busykeyerr.decrRefCount();
    self.space.decrRefCount();
    self.colon.decrRefCount();
    self.plus.decrRefCount();
    for (self.integers) |obj| obj.free();
    for (self.bulkhdr) |obj| obj.decrRefCount();
    for (self.mbulkhdr) |obj| obj.decrRefCount();
    sds.free(self.minstring);
    sds.free(self.maxstring);
    self.* = undefined;
}

const Object = @import("Object.zig");
const sds = @import("sds.zig");
const Server = @import("Server.zig");
