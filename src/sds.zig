const MemSizedHdr5 = struct {
    alloc: u5,
    hdr: Hdr5,
};

pub const Hdr5 = struct {
    flags: u8 align(1), // 3 lsb of type, and 5 msb of string length
    buf: [0]u8,

    inline fn fromString(s: String) *MemSizedHdr5 {
        const hdr: *Hdr5 = @ptrFromInt(@intFromPtr(s) - @sizeOf(Hdr5));
        return @alignCast(@fieldParentPtr("hdr", hdr));
    }

    inline fn setLen(s: String, len: u5) void {
        setType(s, TYPE_5 | (@as(u8, len) << TYPE_BITS));
    }

    inline fn getLen(s: String) u5 {
        const flags = (s - 1)[0];
        return @intCast(flags >> TYPE_BITS);
    }
};

pub const Hdr8 = struct {
    len: u8 align(1),
    alloc: u8 align(1), // excluding the header and null terminator
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,

    inline fn fromString(s: String) *Hdr8 {
        return @ptrFromInt(@intFromPtr(s) - @sizeOf(Hdr8));
    }
};

pub const Hdr16 = struct {
    len: u16 align(1),
    alloc: u16 align(1), // excluding the header and null terminator
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,

    inline fn fromString(s: String) *Hdr16 {
        return @ptrFromInt(@intFromPtr(s) - @sizeOf(Hdr16));
    }
};

pub const Hdr32 = struct {
    len: u32 align(1),
    alloc: u32 align(1), // excluding the header and null terminator
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,

    inline fn fromString(s: String) *Hdr32 {
        return @ptrFromInt(@intFromPtr(s) - @sizeOf(Hdr32));
    }
};

pub const Hdr64 = struct {
    len: u64 align(1),
    alloc: u64 align(1), // excluding the header and null terminator
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,

    inline fn fromString(s: String) *Hdr64 {
        return @ptrFromInt(@intFromPtr(s) - @sizeOf(Hdr64));
    }
};

const MAX_PREALLOC = 1024 * 1024;

pub const TYPE_MASK = 0b00000111;
pub const TYPE_BITS = 3;
pub const TYPE_5 = 0;
pub const TYPE_8 = 1;
pub const TYPE_16 = 2;
pub const TYPE_32 = 3;
pub const TYPE_64 = 4;

pub const String = [*]u8;

pub fn cast(ptr: *anyopaque) String {
    return @ptrCast(ptr);
}

/// Create a new sds string starting from a string.
pub fn new(allocator: Allocator, init: []const u8) String {
    return newLen(allocator, init.ptr, init.len);
}

/// Create an empty (zero length) sds string. Even in this case the string
/// always has an implicit null term.
pub fn empty(allocator: Allocator) String {
    return new(allocator, "");
}

pub const NOINIT: []const u8 = "NOINIT";

/// Create a new sds string with the content specified by the 'init' pointer
/// and 'initlen'.
///
///* If NOINIT is used, the buffer is left uninitialized;
pub fn newLen(allocator: Allocator, init: [*]const u8, initlen: usize) String {
    // Empty strings are usually created in order to append. Use type 8
    // since type 5 is not good at this.
    var typ = reqType(initlen);
    if (typ == TYPE_5 and initlen == 0) {
        typ = TYPE_8;
    }

    var mutinit: ?[*]const u8 = init;
    const hdr_len = hdrSize(typ);
    const mem_size = hdr_len + initlen + 1;
    const mem = allocator.alloc(u8, mem_size) catch oom();
    if (@intFromPtr(init) == @intFromPtr(NOINIT.ptr)) {
        mutinit = null;
    }

    const s: String = mem.ptr + hdr_len;
    setType(s, typ);
    setLength(s, initlen);
    setAlloc(s, initlen);
    if (mutinit) |data| {
        setBuf(s, data[0..initlen]);
    }
    s[initlen] = 0;
    return s;
}

/// Create an sds string from a long long value. It is much faster than:
/// sds.catPrintf(sds.empty(),"{}", .{value});
pub fn fromLonglong(allocator: Allocator, num: i64) String {
    var buf: [20]u8 = undefined;
    const digits = util.ll2string(&buf, num);
    return new(allocator, digits);
}

/// Duplicate an sds string.
pub fn dupe(allocator: Allocator, s: String) String {
    return new(allocator, s[0..getLen(s)]);
}

/// Modify an sds string in-place to make it empty (zero length).
/// However all the existing buffer is not discarded but set as free space
/// so that next append operations will not require allocations up to the
/// number of bytes previously available.
pub fn clear(s: String) void {
    setLength(s, 0);
    s[0] = 0;
}

/// Enlarge the free space at the end of the sds string so that the caller
/// is sure that after calling this function can overwrite up to addlen
/// bytes after the end of the string, plus one more byte for nul term.
///
/// Note: this does not change the *length* of the sds string as returned
/// by getLen(), but only the free buffer space we have.
pub fn makeRoomFor(allocator: Allocator, s: String, add_len: usize) String {
    if (getAvail(s) >= add_len) return s;

    const old_len = getLen(s);
    var new_alloc = old_len + add_len;
    if (new_alloc < MAX_PREALLOC) {
        new_alloc *= 2;
    } else {
        new_alloc += MAX_PREALLOC;
    }
    var new_type = reqType(new_alloc);
    // Don't use type 5: the user is appending to the string and type 5 is
    // not able to remember empty space, so makeRoomFor() must be called
    // at every appending operation.
    if (new_type == TYPE_5) {
        new_type = TYPE_8;
    }
    const new_hdr_len = hdrSize(new_type);
    const new_mem_size = new_hdr_len + new_alloc + 1;

    const old_mem = allocMemSlice(s);
    const old_type = getType(s);
    if (old_type == new_type) {
        const new_mem = allocator.realloc(
            old_mem,
            new_mem_size,
        ) catch oom();
        const ns: [*]u8 = new_mem.ptr + new_hdr_len;
        setAlloc(ns, new_alloc);
        return ns;
    }

    // Since the header size changes, need to move the string forward,
    // and can't use realloc.
    const new_mem = allocator.alloc(u8, new_mem_size) catch oom();
    const ns: String = new_mem.ptr + new_hdr_len;
    setType(ns, new_type);
    setLength(ns, old_len);
    setAlloc(ns, new_alloc);
    setBuf(ns, s[0..old_len]);
    ns[old_len] = 0;
    allocator.free(old_mem);
    return ns;
}

/// Append the specified string to the sds string 's'.
pub fn cat(allocator: Allocator, s: String, src: []const u8) String {
    const cur_len = getLen(s);
    const ns = makeRoomFor(allocator, s, src.len);
    memcpy(ns + cur_len, src, src.len);
    setLength(ns, cur_len + src.len);
    ns[cur_len + src.len] = 0;
    return ns;
}

/// Append to the sds string 's' a string obtained using std.fmt-alike format
/// specifier.
///
/// Example:
///
/// s = sds.new("Sum is: ");
/// s = sds.catPrintf(s,"{}+{} = {}", .{a, b, a+b}).
///
/// Often you need to create a string from scratch with the stf.fmt-alike
/// format. When this is the need, just use sds.empty() as the target string:
///
/// s = sds.catPrintf(sds.empty(), "... your format ...", args)
pub fn catPrintf(
    allocator: Allocator,
    s: String,
    comptime fmt: []const u8,
    args: anytype,
) String {
    var stack_impl = std.heap.stackFallback(
        fmt.len * 10,
        allocator,
    );
    const stack_allocator = stack_impl.get();
    const buf = std.fmt.allocPrint(
        stack_allocator,
        fmt,
        args,
    ) catch oom();
    defer stack_allocator.free(buf);
    return cat(allocator, s, buf);
}

/// Append to the sds string "s" an escaped string representation where
/// all the non-printable characters are turned into escapes in the form
/// "\n\r\a...." or "\x<hex-number>".
pub fn catRepr(allocator: Allocator, s: String, raw: []const u8) String {
    var ns = cat(allocator, s, "\"");

    for (raw) |b| {
        switch (b) {
            '\\', '"' => ns = catPrintf(allocator, ns, "\\{c}", .{b}),
            '\n' => ns = cat(allocator, ns, "\\n"),
            '\r' => ns = cat(allocator, ns, "\\r"),
            '\t' => ns = cat(allocator, ns, "\\t"),
            // in c: '\a'
            0x07 => ns = cat(allocator, ns, "\\a"),
            // in c: '\b'
            0x08 => ns = cat(allocator, ns, "\\b"),
            else => {
                if (std.ascii.isPrint(b)) {
                    ns = catPrintf(allocator, ns, "{c}", .{b});
                } else {
                    ns = catPrintf(allocator, ns, "\\x{x:0>2}", .{b});
                }
            },
        }
    }
    ns = cat(allocator, ns, "\"");
    return ns;
}

/// Reallocate the sds string so that it has no free space at the end. The
/// contained string remains not altered, but next concatenation operations
/// will require a reallocation.
pub fn removeAvailSpace(allocator: Allocator, s: String) String {
    if (getAvail(s) == 0) return s;

    const len = getLen(s);
    const new_type = reqType(len);
    const old_mem = allocMemSlice(s);

    // If the type is the same, or at least a large enough type is still
    // required, we just realloc(), letting the allocator to do the copy
    // only if really needed. Otherwise if the change is huge, we manually
    // reallocate the string to use the different header type.
    const old_type = getType(s);
    if (old_type == new_type or new_type > TYPE_8) {
        const old_hdr_len = hdrSize(old_type);
        const new_mem = allocator.realloc(
            old_mem,
            old_hdr_len + len + 1,
        ) catch oom();
        const ns = new_mem.ptr + old_hdr_len;
        setAlloc(ns, len);
        return ns;
    }

    const new_hdr_len = hdrSize(new_type);
    const new_mem = allocator.alloc(u8, new_hdr_len + len + 1) catch oom();
    const ns = new_mem.ptr + new_hdr_len;
    setType(ns, new_type);
    setLength(ns, len);
    setAlloc(ns, len);
    setBuf(ns, s[0..len]);
    ns[len] = 0;
    allocator.free(old_mem);
    return ns;
}

/// Increment the sds length and decrements the left free space at the
/// end of the string according to 'incr'. Also set the null term
/// in the new end of the string.
///
/// This function is used in order to fix the string length after the
/// user calls makeRoomFor(), writes something after the end of
/// the current string, and finally needs to set the new length.
///
/// Note: it is possible to use a negative increment in order to
/// right-trim the string.
///
/// Usage example:
///
/// Using IncrLen() and makeRoomFor() it is possible to mount the
/// following schema, to cat bytes coming from the kernel to the end of an
/// sds string without copying into an intermediate buffer:
///
/// oldlen = getLen(s);
/// s = makeRoomFor(s, BUFFER_SIZE);
/// nread = read(fd, s+oldlen, BUFFER_SIZE);
/// ... check for nread <= 0 and handle it ...
/// incrLen(s, nread);
pub fn incrLen(s: String, incr: isize) void {
    const len = getLen(s);
    const alloc = getAlloc(s);
    const abs_incr: usize = @abs(incr);

    assert((incr > 0 and alloc - len >= abs_incr) or (incr < 0 and len >= abs_incr));
    const new_len = if (incr > 0) len + abs_incr else len - abs_incr;
    setLength(s, new_len);
    s[new_len] = 0;
}

/// Grow the sds to have the specified length. Bytes that were not part of
/// the original length of the sds will be set to zero.
///
/// if the specified length is smaller than the current length, no operation
/// is performed.
pub fn growZero(allocator: Allocator, s: String, new_len: usize) String {
    const curr_len = getLen(s);
    if (new_len <= curr_len) return s;

    const add_len = new_len - curr_len;
    const ns = makeRoomFor(allocator, s, add_len);
    memset(ns + curr_len, 0, add_len + 1); // also set trailing \0 byte
    setLength(ns, new_len);
    return ns;
}

/// Destructively modify the sds string 's' to hold the specified binary
/// safe string pointed by 'src'.
pub fn copy(allocator: Allocator, s: String, src: []const u8) String {
    var ns = s;
    if (getAlloc(s) < src.len) {
        ns = makeRoomFor(allocator, s, src.len - getLen(s));
    }
    setLength(ns, src.len);
    setBuf(ns, src);
    ns[src.len] = 0;
    return ns;
}

/// Modify the string substituting all the occurrences of the set of
/// characters specified in the 'from' string to the corresponding character
/// in the 'to' array.
///
/// For instance: sds.mapChars(mystring, "ho", "01", 2)
/// will have the effect of turning the string "hello" into "0ell1".
///
/// The function returns the sds string pointer, that is always the same
/// as the input pointer since no resize is needed.
pub fn mapChars(s: String, from: []const u8, to: []const u8) String {
    assert(from.len == to.len);

    const len = getLen(s);
    for (0..len) |j| {
        for (0..from.len) |i| {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

/// Join an array of strings using the specified separator.
/// Returns the result as an sds string.
pub fn join(
    allocator: Allocator,
    slices: []const []const u8,
    sep: []const u8,
) String {
    var joined = empty(allocator);
    for (slices, 0..) |slice, i| {
        joined = cat(allocator, joined, slice);
        if (i != slices.len - 1) joined = cat(allocator, joined, sep);
    }
    return joined;
}

/// Split 's' with separator in 'sep'. An array
/// of sds strings is returned.
///
/// Note that 'sep' is able to split a string using
/// a multi-character separator. For example
/// sds.split("foo_-_bar", "_-_"); will return two
/// elements "foo" and "bar".
pub fn split(allocator: Allocator, str: []const u8, sep: []const u8) []String {
    var tokens = std.ArrayList(String).empty;

    var it = std.mem.splitSequence(u8, str, sep);
    while (it.next()) |token| {
        tokens.append(allocator, new(allocator, token)) catch oom();
    }
    return tokens.toOwnedSlice(allocator) catch oom();
}

/// Split a line into arguments, where every argument can be in the
/// following programming-language REPL-alike form:
///
/// foo bar "newline are supported\n" and "\xff\x00otherstuff"
///
/// An array of arguments of String is returned.
///
/// The caller should free the resulting array of sds strings with
/// freeSplitRes().
///
/// Note that catRepr() is able to convert back a string into
/// a quoted string in the same format splitArgs() is able to parse.
///
/// The function returns the allocated tokens on success, even when the
/// input string is empty, or NULL if the input contains unbalanced
/// quotes or closed quotes followed by non space characters
/// as in: "foo"bar or "foo'
pub fn splitArgs(allocator: Allocator, line: [:0]const u8) ?[]String {
    var vector = std.ArrayList(String).empty;
    var current: ?String = null;

    var p = line.ptr;
    biz: {
        while (true) {
            // skip blanks
            while (p[0] != 0 and isWhitespace(p[0])) p += 1;
            if (p[0] != 0) {
                var inq = false; // set to true if we are in "quotes"
                var insq = false; // set to true if we are in 'single quotes'
                var done = false;

                if (current == null) current = empty(allocator);
                while (!done) {
                    if (inq) {
                        if (p[0] == '\\' and
                            p[1] == 'x' and
                            isHex(p[2]) and
                            isHex(p[3]))
                        {
                            const byte = parseInt(u8, p[2..4], 16) catch {
                                unreachable;
                            };
                            current = cat(allocator, current.?, &.{byte});
                            p += 3;
                        } else if (p[0] == '\\' and p[1] != 0) {
                            p += 1;
                            var c: u8 = undefined;
                            switch (p[0]) {
                                'n' => c = '\n',
                                'r' => c = '\r',
                                't' => c = '\t',
                                'b' => c = 0x08,
                                'a' => c = 0x07,
                                else => c = p[0],
                            }
                            current = cat(allocator, current.?, &.{c});
                        } else if (p[0] == '"') {
                            // closing quote must be followed by a space or
                            // nothing at all.
                            if (p[1] != 0 and !isWhitespace(p[1])) {
                                break :biz;
                            }
                            done = true;
                        } else if (p[0] == 0) {
                            // unterminated quotes
                            break :biz;
                        } else {
                            current = cat(allocator, current.?, &.{p[0]});
                        }
                    } else if (insq) {
                        if (p[0] == '\\' and p[1] == '\'') {
                            p += 1;
                            current = cat(allocator, current.?, "'");
                        } else if (p[0] == '\'') {
                            // closing quote must be followed by a space or
                            // nothing at all.
                            if (p[1] != 0 and !isWhitespace(p[1])) {
                                break :biz;
                            }
                            done = true;
                        } else if (p[0] == 0) {
                            // unterminated quotes
                            break :biz;
                        } else {
                            current = cat(allocator, current.?, &.{p[0]});
                        }
                    } else {
                        switch (p[0]) {
                            ' ', '\n', '\r', '\t', 0 => done = true,
                            '"' => inq = true,
                            '\'' => insq = true,
                            else => current = cat(allocator, current.?, &.{p[0]}),
                        }
                    }
                    if (p[0] != 0) p += 1;
                }
                vector.append(allocator, current.?) catch oom();
                current = null;
                continue;
            }
            return vector.toOwnedSlice(allocator) catch oom();
        }
    }

    // Equivalent to `goto err` in C.
    if (current) |cur| {
        free(allocator, cur);
    }
    for (vector.items) |item| {
        free(allocator, item);
    }
    vector.deinit(allocator);

    return null;
}

/// Free the result returned by sds.split().
pub fn freeSplitRes(allocator: Allocator, tokens: []String) void {
    for (tokens) |token| {
        free(allocator, token);
    }
    allocator.free(tokens);
}

/// Remove the part of the string from left and from right composed just of
/// contiguous characters found in 'values_to_strip'.
///
/// Example:
///
/// s = sds.new("AA...AA.a.aa.aHelloWorld     :::");
/// s = sds.trim(s,"Aa. :");
///
/// Output will be just "HelloWorld".
pub fn trim(s: String, values_to_strip: []const u8) void {
    const trimed = std.mem.trim(u8, asBytes(s), values_to_strip);
    memmove(s, trimed, trimed.len);
    setLength(s, trimed.len);
    s[trimed.len] = 0;
}

/// Turn the string into a smaller (or equal) string containing only the
/// substring specified by the 'start' and 'end' indexes.
///
/// start and end can be negative, where -1 means the last character of the
/// string, -2 the penultimate character, and so forth.
///
/// The interval is inclusive, so the start and end characters will be part
/// of the resulting string.
///
/// The string is modified in-place.
///
/// Example:
///
/// s = new("Hello World");
/// range(s,1,-1); => "ello World"
pub fn range(s: String, start: isize, endinc: isize) void {
    const len = getLen(s);
    if (len == 0) return;

    var from: usize = @abs(start);
    var to: usize = @abs(endinc);

    if (start < 0) {
        if (@abs(start) > len) {
            from = 0;
        } else {
            from = len - @abs(start);
        }
    }
    if (endinc < 0) {
        if (@abs(endinc) > len) {
            to = 0;
        } else {
            to = len - @abs(endinc);
        }
    }

    var new_len = if (from > to) 0 else to - from + 1;
    if (new_len != 0) {
        if (from >= len) {
            new_len = 0;
        } else if (to >= len) {
            to = len - 1;
            new_len = if (from > to) 0 else to - from + 1;
        }
    } else {
        from = 0;
    }

    if (from != 0 and new_len != 0) {
        memmove(s, s + from, new_len);
    }
    setLength(s, new_len);
    s[new_len] = 0;
}

/// Apply toLower() to every character of the sds string 's'.
pub fn toLower(s: String) void {
    const slice = asBytes(s);
    for (slice, 0..) |c, i| {
        slice[i] = std.ascii.toLower(c);
    }
}

/// Apply toUpper() to every character of the sds string 's'.
pub fn toUpper(s: String) void {
    const slice = asBytes(s);
    for (slice, 0..) |c, i| {
        slice[i] = std.ascii.toUpper(c);
    }
}

/// Compare two sds strings s1 and s2.
///
/// Return value:
///
///     .gt if s1 > s2.
///     .lt if s1 < s2.
///     .eq if s1 and s2 are exactly the same binary string.
///
/// If two strings share exactly the same prefix, but one of the two has
/// additional characters, the longer string is considered to be greater than
/// the smaller one.
pub fn cmp(s1: String, s2: String) std.math.Order {
    const lhs = asBytes(s1);
    const rhs = asBytes(s2);

    return std.mem.order(u8, lhs, rhs);
}

/// This is just a wrapper to cmp() that is able to handle min and max as the
/// equivalent of -inf and +inf for strings.
pub fn cmplex(a: String, b: String, min: String, max: String) std.math.Order {
    if (a == b) return .eq;
    if (a == min or b == max) {
        return .lt;
    }
    if (a == max or b == min) {
        return .gt;
    }
    return cmp(a, b);
}

/// Compare two sds strings s1 and s2  in a case–insensitive way.
///
/// Return value:
///
///     .gt if s1 > s2.
///     .lt if s1 < s2.
///     .eq if s1 and s2 are exactly the same binary string.
///
/// If two strings share exactly the same prefix, but one of the two has
/// additional characters, the longer string is considered to be greater than
/// the smaller one.
pub fn caseCmp(s1: String, s2: String) std.math.Order {
    const lhs = asBytes(s1);
    const rhs = asBytes(s2);

    const n = @min(lhs.len, rhs.len);
    for (lhs[0..n], rhs[0..n]) |lhs_elem, rhs_elem| {
        const l = std.ascii.toLower(lhs_elem);
        const r = std.ascii.toLower(rhs_elem);
        switch (std.math.order(l, r)) {
            .eq => continue,
            .lt => return .lt,
            .gt => return .gt,
        }
    }
    return std.math.order(lhs.len, rhs.len);
}

pub fn eql(s1: String, s2: String) bool {
    return cmp(s1, s2) == .eq;
}

pub fn eqlCase(s1: String, s2: String) bool {
    return caseCmp(s1, s2) == .eq;
}

pub fn hash(s: String) hasher.Hash {
    return hasher.hash(asBytes(s));
}

pub fn asLongLong(s: String) ?i64 {
    return std.fmt.parseInt(i64, asBytes(s), 10) catch {
        return null;
    };
}

/// Return the length of the sds string `s`, excluding the null terminator.
pub fn getLen(s: String) usize {
    const flags = (s - 1)[0];
    return switch (flags & TYPE_MASK) {
        TYPE_5 => Hdr5.getLen(s),
        TYPE_8 => Hdr8.fromString(s).len,
        TYPE_16 => Hdr16.fromString(s).len,
        TYPE_32 => Hdr32.fromString(s).len,
        TYPE_64 => Hdr64.fromString(s).len,
        else => 0,
    };
}

/// Return how many bytes can still be appended to `s` without reallocating.
pub fn getAvail(s: String) usize {
    return getAlloc(s) - getLen(s);
}

/// Return the total capacity available for storing content in `s`.
pub fn getAlloc(s: String) usize {
    const flags = (s - 1)[0];
    return switch (flags & TYPE_MASK) {
        TYPE_5 => Hdr5.fromString(s).alloc,
        TYPE_8 => Hdr8.fromString(s).alloc,
        TYPE_16 => Hdr16.fromString(s).alloc,
        TYPE_32 => Hdr32.fromString(s).alloc,
        TYPE_64 => Hdr64.fromString(s).alloc,
        else => 0,
    };
}

/// Return the total size of the allocation of the specified sds string,
/// including:
/// 1) The sds header before the pointer.
/// 2) The string.
/// 3) The free buffer at the end if any.
/// 4) The implicit null term.
pub fn allocSize(s: String) usize {
    return hdrSize(getType(s)) + getAlloc(s) + 1;
}

pub inline fn asBytes(s: String) []u8 {
    return s[0..getLen(s)];
}

pub inline fn castBytes(ptr: *anyopaque) []u8 {
    return asBytes(cast(ptr));
}

pub inline fn asSentinelBytes(s: String) [:0]u8 {
    return s[0..getLen(s) :0];
}

pub fn setLength(s: String, new_len: usize) void {
    switch (getType(s)) {
        TYPE_5 => Hdr5.setLen(s, @intCast(new_len)),
        TYPE_8 => Hdr8.fromString(s).len = @intCast(new_len),
        TYPE_16 => Hdr16.fromString(s).len = @intCast(new_len),
        TYPE_32 => Hdr32.fromString(s).len = @intCast(new_len),
        TYPE_64 => Hdr64.fromString(s).len = @intCast(new_len),
        else => {},
    }
}

/// Free an sds string.
pub fn free(allocator: Allocator, s: String) void {
    allocator.free(allocMemSlice(s));
}

inline fn getType(s: String) u8 {
    return (s - 1)[0] & TYPE_MASK;
}

inline fn setType(s: String, typ: u8) void {
    (s - 1)[0] = typ;
}

inline fn setBuf(s: String, buf: []const u8) void {
    memcpy(s, buf, buf.len);
}

inline fn allocMemSlice(s: String) []u8 {
    const mem: [*]u8 = s - hdrSize(getType(s));
    return mem[0..allocSize(s)];
}

fn setAlloc(s: String, new_alloc: usize) void {
    switch (getType(s)) {
        TYPE_5 => Hdr5.fromString(s).alloc = @intCast(new_alloc),
        TYPE_8 => Hdr8.fromString(s).alloc = @intCast(new_alloc),
        TYPE_16 => Hdr16.fromString(s).alloc = @intCast(new_alloc),
        TYPE_32 => Hdr32.fromString(s).alloc = @intCast(new_alloc),
        TYPE_64 => Hdr64.fromString(s).alloc = @intCast(new_alloc),
        else => {},
    }
}

fn reqType(size: usize) u8 {
    if (size < 1 << 5) return TYPE_5;
    if (size < 1 << 8) return TYPE_8;
    if (size < 1 << 16) return TYPE_16;
    if (builtin.target.ptrBitWidth() == 64) {
        if (size < 1 << 32) {
            return TYPE_32;
        }
        return TYPE_64;
    }
    return TYPE_32;
}

fn hdrSize(typ: u8) usize {
    return switch (typ & TYPE_MASK) {
        TYPE_5 => @sizeOf(MemSizedHdr5),
        TYPE_8 => @sizeOf(Hdr8),
        TYPE_16 => @sizeOf(Hdr16),
        TYPE_32 => @sizeOf(Hdr32),
        TYPE_64 => @sizeOf(Hdr64),
        else => 0,
    };
}

test new {
    const allocator = std.testing.allocator;
    const init = "hello";
    const s = new(allocator, init);
    defer free(allocator, s);

    try expectEqual(init.len, getLen(s));
    try expectEqualStrings(init, asBytes(s));

    const long_str = "hello" ** 100;
    const long_s = new(allocator, long_str);
    defer free(allocator, long_s);
    try expectEqual(long_str.len, getLen(long_s));
    try expectEqualStrings(long_str, asBytes(long_s));
}

test empty {
    const allocator = std.testing.allocator;
    const s = empty(allocator);
    defer free(allocator, s);

    try expectEqual(0, getLen(s));
    try expectStringEndsWith("", asBytes(s));
}

test fromLonglong {
    const allocator = std.testing.allocator;
    const min = fromLonglong(allocator, std.math.minInt(i64));
    defer free(allocator, min);
    try expectEqualStrings("-9223372036854775808", asBytes(min));

    const max = fromLonglong(allocator, std.math.maxInt(i64));
    defer free(allocator, max);
    try expectEqualStrings("9223372036854775807", asBytes(max));
}

test dupe {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    defer free(allocator, s);

    const dup = dupe(allocator, s);
    defer free(allocator, dup);

    try expectEqualStrings(asBytes(s), asBytes(dup));
}

test clear {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    defer free(allocator, s);

    clear(s);
    try expectEqual(0, getLen(s));
    try expectEqualStrings("", asBytes(s));
}

test makeRoomFor {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    const s1 = makeRoomFor(allocator, s, 500);
    defer free(allocator, s1);

    try expectEqual(5, getLen(s1));
    try expectEqualStrings("hello", asBytes(s1));
}

test removeAvailSpace {
    const allocator = std.testing.allocator;
    var s = new(allocator, "hello");
    defer free(allocator, s);

    s = makeRoomFor(allocator, s, 5);
    s = removeAvailSpace(allocator, s);
    try expect(getLen(s) == 5);
    try expect(getAlloc(s) == 5);
    try expect(getAvail(s) == 0);
}

test cat {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    const ns = cat(allocator, s, "world");
    defer free(allocator, ns);

    try expectEqual(10, getLen(ns));
    try expectEqualStrings("helloworld", asBytes(ns));
}

test catPrintf {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    const ns = catPrintf(allocator, s, " {s} {d}", .{ "world", 2025 });
    defer free(allocator, ns);
    try expectEqualStrings("hello world 2025", asBytes(ns));
}

test catRepr {
    const allocator = std.testing.allocator;
    const s = empty(allocator);
    const input = "\x07\n\x00foo\r\""; // in c: \a\n\0foo\r\"
    const ns = catRepr(allocator, s, input);
    defer free(allocator, ns);

    try expectEqualStrings("\"\\a\\n\\x00foo\\r\\\"\"", asBytes(ns));
}

test allocSize {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    defer free(allocator, s);
    try expectEqual(@sizeOf(MemSizedHdr5) + 5 + 1, allocSize(s));
}

test "incrLen-" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");
    defer free(allocator, s);
    incrLen(s, -3);

    try expectEqual(2, getLen(s));
}

test "incrLen+" {
    const allocator = std.testing.allocator;
    var s = new(allocator, "hello");
    defer free(allocator, s);
    s = makeRoomFor(allocator, s, 3);
    incrLen(s, 3);
    try expectEqual(8, getLen(s));
}

test growZero {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");

    const ns = growZero(allocator, s, 10);
    defer free(allocator, ns);

    try expectEqual(10, getLen(ns));
    try expectEqualSlices(
        u8,
        &.{ 'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0 },
        asBytes(ns),
    );
}

test copy {
    const allocator = std.testing.allocator;
    const s = new(allocator, "hello");

    var cp = copy(allocator, s, "world");
    try expectEqual(getLen(cp), 5);
    try expectEqualStrings("world", asBytes(cp));
    try expectEqual(s, cp);

    cp = copy(allocator, cp, "world!");
    defer free(allocator, cp);
    try expectEqual(getLen(cp), 6);
    try expectEqualStrings("world!", asBytes(cp));
    try expect(s != cp);
}

test mapChars {
    const allocator = std.testing.allocator;
    var s = new(allocator, "hello\r\nzig");
    defer free(allocator, s);

    s = mapChars(s, "eoi\r\n", "EOI  ");
    try expectEqualSlices(u8, "hEllO  zIg", asBytes(s));
}

test join {
    const allocator = std.testing.allocator;
    var tokens = std.ArrayList([]const u8).empty;
    defer tokens.deinit(allocator);

    tokens.append(allocator, "hello") catch oom();
    tokens.append(allocator, "world") catch oom();
    tokens.append(allocator, "zig") catch oom();

    const joined = join(allocator, tokens.items, "|");
    defer free(allocator, joined);

    try expectEqualSlices(u8, "hello|world|zig", asBytes(joined));
}

test split {
    const allocator = std.testing.allocator;
    const tokens = split(allocator, "hello|world|zig", "|");
    defer freeSplitRes(allocator, tokens);

    try expect(tokens.len == 3);
    try expectEqualStrings("hello", asBytes(tokens[0]));
    try expectEqualStrings("world", asBytes(tokens[1]));
    try expectEqualStrings("zig", asBytes(tokens[2]));
}

test splitArgs {
    const allocator = std.testing.allocator;
    var tokens = (splitArgs(allocator, " \n SET  \r key \t 1")).?;
    try expect(tokens.len == 3);
    try expectEqualStrings("SET", asBytes(tokens[0]));
    try expectEqualStrings("key", asBytes(tokens[1]));
    try expectEqualStrings("1", asBytes(tokens[2]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "\"1\"")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("1", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "\"line1\\nline2\"")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("line1\nline2", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "\"ABC\\x41XYZ\"")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("ABCAXYZ", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "'abc'")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("abc", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "'I\\'m here'")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("I'm here", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "\"a\\tb\"")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("a\tb", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "\"abc\\xGZ\"")).?;
    try expect(tokens.len == 1);
    try expectEqualStrings("abcxGZ", asBytes(tokens[0]));
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "")).?;
    try expect(tokens.len == 0);
    freeSplitRes(allocator, tokens);

    tokens = (splitArgs(allocator, "   ")).?;
    try expect(tokens.len == 0);
    freeSplitRes(allocator, tokens);

    var null_tokens: ?[]String = splitArgs(allocator, "  SET key 'abc");
    try expect(null_tokens == null);

    null_tokens = splitArgs(allocator, "  SET key \"foo\"bar");
    try expect(null_tokens == null);
}

test trim {
    const allocator = std.testing.allocator;
    const s = new(allocator, "AA...AA.a.aa.aHelloWorld     :::");
    defer free(allocator, s);

    trim(s, "Aa. :");
    try expectEqualStrings("HelloWorld", asBytes(s));

    trim(s, "d");
    try expectEqualStrings("HelloWorl", asBytes(s));
}

test "range(1, 1)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 1, 1);

    try expectEqualStrings("h", asBytes(s));
}

test "range(1, -1)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 1, -1);

    try expectEqualStrings("hello!", asBytes(s));
}

test "range(-2, -1)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, -2, -1);

    try expectEqualStrings("o!", asBytes(s));
}

test "range(2, 1)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 2, 1);

    try expectEqualStrings("", asBytes(s));
}

test "range(1, 100)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 1, 100);

    try expectEqualStrings("hello!", asBytes(s));
}

test "range(100, 100)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 100, 100);

    try expectEqualStrings("", asBytes(s));
}

test "range(0, 1)" {
    const allocator = std.testing.allocator;
    const s = new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 0, 1);

    try expectEqualStrings("!h", asBytes(s));
}

test toLower {
    const allocator = std.testing.allocator;
    const s = new(allocator, "Hello1");
    defer free(allocator, s);
    toLower(s);

    try expectEqualStrings("hello1", asBytes(s));
}

test toUpper {
    const allocator = std.testing.allocator;
    const s = new(allocator, "Hello1");
    defer free(allocator, s);
    toUpper(s);

    try expectEqualStrings("HELLO1", asBytes(s));
}

test "cmp.gt" {
    const allocator = std.testing.allocator;
    const s1 = new(allocator, "foo");
    defer free(allocator, s1);

    const s2 = new(allocator, "foa");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.gt, cmp(s1, s2));
}

test "cmp.eq" {
    const allocator = std.testing.allocator;
    const s1 = new(allocator, "bar");
    defer free(allocator, s1);

    const s2 = new(allocator, "bar");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.eq, cmp(s1, s2));
}

test "cmp.lt" {
    const allocator = std.testing.allocator;
    const s1 = new(allocator, "aar");
    defer free(allocator, s1);

    const s2 = new(allocator, "bar");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.lt, cmp(s1, s2));
}

test "cmpCase.gt" {
    const allocator = std.testing.allocator;
    const s1 = new(allocator, "Foo");
    defer free(allocator, s1);

    const s2 = new(allocator, "foA");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.gt, caseCmp(s1, s2));
}

test "cmpCase.eq" {
    const allocator = std.testing.allocator;
    const s1 = new(allocator, "bar");
    defer free(allocator, s1);

    const s2 = new(allocator, "Bar");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.eq, caseCmp(s1, s2));
}

test "cmpCase.lt" {
    const allocator = std.testing.allocator;
    const s1 = new(allocator, "aar");
    defer free(allocator, s1);

    const s2 = new(allocator, "Bar");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.lt, caseCmp(s1, s2));
}

test asLongLong {
    const allocator = std.testing.allocator;
    var s = new(allocator, "123456789");
    defer free(allocator, s);

    const llval = asLongLong(s);
    try expect(llval != null);
    try expectEqual(123456789, llval.?);

    clear(s);
    s = cat(allocator, s, "abc1");
    try expect(asLongLong(s) == null);
}

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;
const expect = testing.expect;
const expectEqualStrings = testing.expectEqualStrings;
const expectEqual = testing.expectEqual;
const expectEqualSlices = testing.expectEqualSlices;
const expectStringEndsWith = testing.expectStringEndsWith;
const isWhitespace = std.ascii.isWhitespace;
const isHex = std.ascii.isHex;
const parseInt = std.fmt.parseInt;
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const memset = memzig.memset;
const memmove = memzig.memmove;
const util = @import("util.zig");
const Allocator = std.mem.Allocator;
const oom = @import("allocator.zig").oom;
const hasher = @import("hasher.zig");
