/// Context must be a struct type with three member functions:
///
///     fn eql(self, SearchKey, Value) bool
///
///     fn dupe(self, Value) !Value
///
///     fn free(self, Value) void
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
                    self.direction = Direction.forward;
                    return;
                }
                self.curr = self.list.last;
                self.direction = Direction.backward;
            }
        };

        first: ?*Node = null,
        last: ?*Node = null,
        len: usize = 0,
        ctx: Context,

        pub fn init(ctx: Context) List {
            return .{
                .ctx = ctx,
            };
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
            defer self.ctx.free(existing_node.value);

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

        pub fn rotate(self: *List) void {
            if (self.len <= 1) return;

            const tail = self.last.?;
            self.last = tail.prev;
            self.last.?.next = null;

            self.first.?.prev = tail;
            tail.prev = null;
            tail.next = self.first;
            self.first = tail;
        }

        pub fn iterator(self: *List, direction: Iterator.Direction) Iterator {
            return .init(self, direction);
        }

        pub fn dupe(
            self: *List,
            allocator: Allocator,
        ) Allocator.Error!List {
            var dup_list: List = .init(self.ctx);
            errdefer dup_list.deinit(allocator);

            var it = self.iterator(.forward);
            while (it.next()) |node| {
                const dup_value = try self.ctx.dupe(node.value);
                errdefer self.ctx.free(dup_value);
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

        pub fn index(self: *List, idx: isize) ?*Node {
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

        pub fn deinit(self: *List, allocator: Allocator) void {
            var curr = self.first;
            while (curr) |node| {
                curr = node.next;
                self.ctx.free(node.value);
                node.destroy(allocator);
            }
            self.* = undefined;
        }
    };
}

test "prepend/insertBefore" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.prepend(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.prepend(allocator, v2);

    try testing.expectEqual(2, ll.len);
    try testing.expectEqual(@intFromPtr(v2), @intFromPtr(ll.first.?.value));
}

test "append/insertAfter" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    try testing.expectEqual(2, ll.len);
    try testing.expectEqual(v2, ll.last.?.value);
}

test "removeNode" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    const first_node = ll.first.?;
    ll.removeNode(allocator, first_node);

    try testing.expectEqual(1, ll.len);
    try testing.expectEqual(v2, ll.first.?.value);
}

test "rotate" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    const v3 = UnitTestContext.create(allocator, 3);
    try ll.append(allocator, v3);

    ll.rotate();

    try testing.expectEqual(3, ll.len);
    try testing.expectEqual(v3, ll.first.?.value);
    try testing.expectEqual(v2, ll.last.?.value);

    var it = ll.iterator(.forward);
    try testing.expectEqual(v3, it.next().?.value);
    try testing.expectEqual(v1, it.next().?.value);
    try testing.expectEqual(v2, it.next().?.value);
    try testing.expectEqual(null, it.next());
}

test "iterator(forward|backward) || Iterator.rewind(forward|backward)" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    const v3 = UnitTestContext.create(allocator, 3);
    try ll.append(allocator, v3);

    var forward = ll.iterator(.forward);
    try testing.expectEqual(v1, forward.next().?.value);
    try testing.expectEqual(v2, forward.next().?.value);
    try testing.expectEqual(v3, forward.next().?.value);
    try testing.expectEqual(null, forward.next());

    forward.rewind(.backward);
    try testing.expectEqual(v3, forward.next().?.value);
    try testing.expectEqual(v2, forward.next().?.value);
    try testing.expectEqual(v1, forward.next().?.value);
    try testing.expectEqual(null, forward.next());

    var backward = ll.iterator(.backward);
    try testing.expectEqual(v3, backward.next().?.value);
    try testing.expectEqual(v2, backward.next().?.value);
    try testing.expectEqual(v1, backward.next().?.value);
    try testing.expectEqual(null, backward.next());

    backward.rewind(.forward);
    try testing.expectEqual(v1, backward.next().?.value);
    try testing.expectEqual(v2, backward.next().?.value);
    try testing.expectEqual(v3, backward.next().?.value);
    try testing.expectEqual(null, backward.next());
}

test "dupe" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    const v3 = UnitTestContext.create(allocator, 3);
    try ll.append(allocator, v3);

    var dup = try ll.dupe(allocator);
    defer dup.deinit(allocator);

    try testing.expectEqual(3, dup.len);

    var it1 = ll.iterator(.forward);
    var it2 = dup.iterator(.forward);

    for (0..dup.len) |_| {
        const n1 = it1.next();
        const n2 = it2.next();

        try testing.expect(n1 != n2);
        try testing.expect(n1.?.value != n2.?.value);
        try testing.expectEqual(n1.?.value.*, n2.?.value.*);
    }
}

test "search" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    const v3 = UnitTestContext.create(allocator, 3);
    try ll.append(allocator, v3);

    const found = ll.search(3);
    try testing.expect(found != null);
    try testing.expectEqual(v3, found.?.value);
    try testing.expectEqual(3, found.?.value.*);
}

test "index" {
    const allocator = testing.allocator;

    var ll: UnitTestList = .init(.init(allocator));
    defer ll.deinit(allocator);

    const v1 = UnitTestContext.create(allocator, 1);
    try ll.append(allocator, v1);

    const v2 = UnitTestContext.create(allocator, 2);
    try ll.append(allocator, v2);

    const v3 = UnitTestContext.create(allocator, 3);
    try ll.append(allocator, v3);

    var found = ll.index(0);
    try testing.expect(found != null and found.?.value == v1);

    found = ll.index(-1);
    try testing.expect(found != null and found.?.value == v3);

    found = ll.index(-2);
    try testing.expect(found != null and found.?.value == v2);

    found = ll.index(1000);
    try testing.expect(found == null);

    found = ll.index(-1000);
    try testing.expect(found == null);
}

const UnitTestList = DoublyLinkedList(
    u32,
    *u32,
    UnitTestContext,
);

const UnitTestContext = struct {
    const Self = @This();

    allocator: Allocator,

    fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    fn create(allocator: Allocator, v: u32) *u32 {
        const ptr = allocator.create(u32) catch unreachable;
        ptr.* = v;
        return ptr;
    }

    fn eql(self: *Self, key: u32, value: *u32) bool {
        _ = self;
        return key == value.*;
    }

    fn dupe(
        self: *Self,
        value: *u32,
    ) Allocator.Error!*u32 {
        const ptr = try self.allocator.create(u32);
        ptr.* = value.*;
        return ptr;
    }

    fn free(self: *Self, value: *u32) void {
        self.allocator.destroy(value);
    }
};

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
