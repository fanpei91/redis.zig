/// Context must be a struct type with the following member functions:
///
///     pub fn eql(self, SearchKey, Value) bool
///
///     pub fn dupe(self, Allocator, Value) Allocator.Error!Value
///
///     pub fn free(self, Allocator, Value) void
pub fn DoublyLinkedList(
    comptime SearchKey: type,
    comptime Value: type,
    comptime Context: type,
) type {
    return struct {
        pub const List = @This();
        pub const Node = struct {
            value: Value,
            prev: ?*Node = null,
            next: ?*Node = null,

            fn create(
                allocator: Allocator,
                value: Value,
            ) Allocator.Error!*Node {
                const n = try allocator.create(Node);
                n.* = .{
                    .value = value,
                };
                return n;
            }

            fn destroy(self: *Node, allocator: Allocator) void {
                allocator.destroy(self);
            }
        };
        pub const Iterator = struct {
            pub const Direction = enum {
                forward,
                backward,
            };

            list: *List,
            curr: ?*Node = null,
            direction: Direction,

            fn init(list: *List, direction: Direction) Iterator {
                var self: Iterator = .{
                    .list = list,
                    .direction = direction,
                };
                if (direction == .forward) {
                    self.curr = list.first;
                } else {
                    self.curr = list.last;
                }
                return self;
            }

            pub fn next(self: *Iterator) ?*Node {
                if (self.curr) |node| {
                    if (self.direction == .forward) {
                        self.curr = node.next;
                    } else {
                        self.curr = node.prev;
                    }
                    return node;
                }
                return null;
            }

            pub fn rewind(self: *Iterator, direction: Direction) void {
                if (direction == .forward) {
                    self.curr = self.list.first;
                    self.direction = .forward;
                    return;
                }
                self.curr = self.list.last;
                self.direction = .backward;
            }
        };

        first: ?*Node = null,
        last: ?*Node = null,
        len: ulong = 0,
        ctx: Context,

        pub fn create(
            allocator: Allocator,
            ctx: Context,
        ) Allocator.Error!*List {
            const list = try allocator.create(List);
            list.* = .{
                .ctx = ctx,
            };
            return list;
        }

        pub fn prepend(
            self: *List,
            allocator: Allocator,
            value: Value,
        ) Allocator.Error!void {
            if (self.first) |first| {
                try self.insertBefore(allocator, value, first);
                return;
            }
            const node = try Node.create(allocator, value);
            self.first = node;
            self.last = node;
            self.len += 1;
        }

        pub fn append(
            self: *List,
            allocator: Allocator,
            value: Value,
        ) Allocator.Error!void {
            if (self.last) |last| {
                try self.insertAfter(allocator, value, last);
                return;
            }
            const node = try Node.create(allocator, value);
            self.first = node;
            self.last = node;
            self.len += 1;
        }

        pub fn insertBefore(
            self: *List,
            allocator: Allocator,
            value: Value,
            existing_node: *Node,
        ) Allocator.Error!void {
            const node = try Node.create(allocator, value);
            node.next = existing_node;
            if (existing_node.prev) |prev| {
                prev.next = node;
                node.prev = prev;
            } else {
                self.first = node;
            }
            existing_node.prev = node;
            self.len += 1;
        }

        pub fn insertAfter(
            self: *List,
            allocator: Allocator,
            value: Value,
            existing_node: *Node,
        ) Allocator.Error!void {
            const node = try Node.create(allocator, value);
            node.prev = existing_node;
            if (existing_node.next) |next| {
                node.next = next;
                next.prev = node;
            } else {
                self.last = node;
            }
            existing_node.next = node;
            self.len += 1;
        }

        pub fn removeNode(
            self: *List,
            allocator: Allocator,
            existing_node: *Node,
        ) void {
            defer existing_node.destroy(allocator);
            defer self.ctx.free(allocator, existing_node.value);

            const prev = existing_node.prev;
            const next = existing_node.next;

            if (prev) |prv| {
                prv.next = next;
            } else {
                self.first = next;
            }

            if (next) |nxt| {
                nxt.prev = prev;
            } else {
                self.last = prev;
            }

            self.len -= 1;
        }

        pub fn join(self: *List, other: *List) void {
            if (other.len == 0) return;
            assert(self != other);

            other.first.?.prev = self.last;
            if (self.last) |last| {
                last.next = other.first;
            } else {
                self.first = other.first;
            }
            self.last = other.last;
            self.len += other.len;

            other.first = null;
            other.last = null;
            other.len = 0;
        }

        pub fn rotateTailToHead(self: *List) void {
            if (self.len <= 1) return;

            const tail = self.last.?;
            self.last = tail.prev;
            self.last.?.next = null;

            self.first.?.prev = tail;
            tail.prev = null;
            tail.next = self.first;
            self.first = tail;
        }

        pub fn rotateHeadToTail(self: *List) void {
            if (self.len <= 1) return;

            const head = self.first.?;
            self.first = head.next;
            self.first.?.prev = null;

            self.last.?.next = head;
            head.prev = self.last;
            head.next = null;
            self.last = head;
        }

        pub fn iterator(self: *List, direction: Iterator.Direction) Iterator {
            return .init(self, direction);
        }

        pub fn dupe(
            self: *List,
            allocator: Allocator,
        ) Allocator.Error!*List {
            var dup_list = try List.create(allocator, self.ctx);
            errdefer dup_list.release(allocator);

            var it = self.iterator(.forward);
            while (it.next()) |node| {
                const dup_value = try self.ctx.dupe(allocator, node.value);
                errdefer self.ctx.free(allocator, dup_value);
                try dup_list.append(allocator, dup_value);
            }
            return dup_list;
        }

        pub fn search(self: *List, key: SearchKey) ?*Node {
            var it = self.iterator(.forward);
            while (it.next()) |node| {
                if (self.ctx.eql(key, node.value)) {
                    return node;
                }
            }
            return null;
        }

        pub fn index(self: *List, idx: long) ?*Node {
            var node: ?*Node = null;
            if (idx < 0) {
                var i = (-idx) - 1;
                node = self.last;
                while (i > 0 and node != null) : (i -= 1) {
                    node = node.?.prev;
                }
            } else {
                var i = idx;
                node = self.first;
                while (i > 0 and node != null) : (i -= 1) {
                    node = node.?.next;
                }
            }
            return node;
        }

        pub fn empty(self: *List, allocator: Allocator) void {
            var curr = self.first;
            while (curr) |node| {
                curr = node.next;
                self.ctx.free(allocator, node.value);
                node.destroy(allocator);
            }
            self.len = 0;
            self.first = null;
            self.last = null;
        }

        pub fn release(self: *List, allocator: Allocator) void {
            self.empty(allocator);
            allocator.destroy(self);
        }
    };
}

test "prepend/insertBefore" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.prepend(allocator, 1);
    try ll.prepend(allocator, 2);

    try expectEqual(2, ll.len);
    try expectEqual(2, ll.first.?.value);

    try ll.insertBefore(allocator, 3, ll.last.?);
    try expectEqual(ll.first.?.next.?.value, 3);
}

test "append/insertAfter" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);

    try expectEqual(2, ll.len);
    try expectEqual(2, ll.last.?.value);

    try ll.insertAfter(allocator, 3, ll.first.?);
    try expectEqual(3, ll.first.?.next.?.value);
}

test "removeNode" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    const node2 = ll.first.?.next.?;
    ll.removeNode(allocator, node2);

    try expectEqual(2, ll.len);
    try expectEqual(1, ll.first.?.value);
    try expectEqual(3, ll.last.?.value);
}

test "rotateTailToHead" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    ll.rotateTailToHead();

    try expectEqual(3, ll.len);
    try expectEqual(3, ll.first.?.value);
    try expectEqual(2, ll.last.?.value);

    var it = ll.iterator(.forward);
    try expectEqual(3, it.next().?.value);
    try expectEqual(1, it.next().?.value);
    try expectEqual(2, it.next().?.value);
    try expectEqual(null, it.next());
}

test "rotateHeadToTail" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    ll.rotateHeadToTail();

    try expectEqual(3, ll.len);
    try expectEqual(2, ll.first.?.value);
    try expectEqual(1, ll.last.?.value);

    var it = ll.iterator(.forward);
    try expectEqual(2, it.next().?.value);
    try expectEqual(3, it.next().?.value);
    try expectEqual(1, it.next().?.value);
    try expectEqual(null, it.next());
}

test "iterator(forward|backward) || Iterator.rewind(forward|backward)" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    var forward = ll.iterator(.forward);
    try expectEqual(1, forward.next().?.value);
    try expectEqual(2, forward.next().?.value);
    try expectEqual(3, forward.next().?.value);
    try expectEqual(null, forward.next());

    forward.rewind(.backward);
    try expectEqual(3, forward.next().?.value);
    try expectEqual(2, forward.next().?.value);
    try expectEqual(1, forward.next().?.value);
    try expectEqual(null, forward.next());

    var backward = ll.iterator(.backward);
    try expectEqual(3, backward.next().?.value);
    try expectEqual(2, backward.next().?.value);
    try expectEqual(1, backward.next().?.value);
    try expectEqual(null, backward.next());

    backward.rewind(.forward);
    try expectEqual(1, backward.next().?.value);
    try expectEqual(2, backward.next().?.value);
    try expectEqual(3, backward.next().?.value);
    try expectEqual(null, backward.next());
}

test "dupe" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    var dup = try ll.dupe(allocator);
    defer dup.release(allocator);

    try expectEqual(3, dup.len);

    var it1 = ll.iterator(.forward);
    var it2 = dup.iterator(.forward);

    for (0..dup.len) |_| {
        const n1 = it1.next();
        const n2 = it2.next();

        try expect(n1 != n2);
        try expectEqual(n1.?.value, n2.?.value);
    }
}

test "search" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    const found = ll.search(3);
    try expect(found != null);
    try expectEqual(3, found.?.value);
    try expectEqual(3, found.?.value);
}

test "index" {
    const allocator = testing.allocator;

    var ll: *TestList = try .create(allocator, .init());
    defer ll.release(allocator);

    try ll.append(allocator, 1);
    try ll.append(allocator, 2);
    try ll.append(allocator, 3);

    var found = ll.index(0);
    try expect(found != null and found.?.value == 1);

    found = ll.index(-1);
    try expect(found != null and found.?.value == 3);

    found = ll.index(-2);
    try expect(found != null and found.?.value == 2);

    found = ll.index(1000);
    try expect(found == null);

    found = ll.index(-1000);
    try expect(found == null);
}

test "join" {
    const allocator = testing.allocator;

    var l1: *TestList = try .create(allocator, .init());
    defer l1.release(allocator);

    var l2: *TestList = try .create(allocator, .init());
    defer l2.release(allocator);

    try l1.append(allocator, 1);
    try l1.append(allocator, 2);
    try l2.append(allocator, 3);
    try l2.append(allocator, 4);

    l1.join(l2);

    try expectEqual(4, l1.len);
    try expectEqual(0, l2.len);
    try expectEqual(null, l2.first);
    try expectEqual(null, l2.last);
    try expectEqual(1, l1.first.?.value);
    try expectEqual(4, l1.last.?.value);

    var it = l1.iterator(.forward);
    try expectEqual(1, it.next().?.value);
    try expectEqual(2, it.next().?.value);
    try expectEqual(3, it.next().?.value);
    try expectEqual(4, it.next().?.value);
    try expectEqual(null, it.next());
}

const TestList = DoublyLinkedList(
    u32,
    u32,
    TestContext,
);

const TestContext = struct {
    const Self = @This();

    fn init() Self {
        return .{};
    }

    fn eql(self: Self, key: u32, value: u32) bool {
        _ = self;
        return key == value;
    }

    fn dupe(_: Self, _: Allocator, value: u32) Allocator.Error!u32 {
        return value;
    }

    fn free(_: Self, _: Allocator, _: u32) void {}
};

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const ctypes = @import("ctypes.zig");
const ulong = ctypes.ulong;
const long = ctypes.long;
const assert = std.debug.assert;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
