pub fn List(
    comptime SearchKey: type,
    comptime Value: type,
) type {
    return struct {
        pub const Vtable = struct {
            eql: ?*const fn (key: SearchKey, val: Value) bool = null,
            dupe: ?*const fn (val: Value) Value = null,
            free: ?*const fn (val: Value) void = null,
        };

        pub const Node = struct {
            value: Value,
            prev: ?*Node = null,
            next: ?*Node = null,

            fn create(value: Value) *Node {
                const n = allocator.create(Node);
                n.* = .{
                    .value = value,
                };
                return n;
            }

            fn destroy(self: *Node) void {
                allocator.destroy(self);
            }
        };
        pub const Iterator = struct {
            pub const Direction = enum {
                forward,
                backward,
            };

            list: *LinkedList,
            curr: ?*Node = null,
            direction: Direction,

            fn init(list: *LinkedList, direction: Direction) Iterator {
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

        const Context = struct {
            vtable: *const Vtable,

            fn eql(self: *Context, key: SearchKey, val: Value) bool {
                if (self.vtable.eql) |cmp| {
                    return cmp(key, val);
                }
                return std.meta.eql(key, val);
            }

            fn dupe(self: *Context, val: Value) Value {
                if (self.vtable.dupe) |d| {
                    return d(val);
                }
                return val;
            }

            fn free(self: *Context, val: Value) void {
                if (self.vtable.free) |f| {
                    f(val);
                }
            }
        };

        const LinkedList = @This();
        first: ?*Node = null,
        last: ?*Node = null,
        len: usize = 0,
        ctx: Context,

        pub fn create(vtable: *const Vtable) *LinkedList {
            const list = allocator.create(LinkedList);
            list.* = .{
                .ctx = .{ .vtable = vtable },
            };
            return list;
        }

        pub fn prepend(self: *LinkedList, value: Value) void {
            if (self.first) |first| {
                self.insertBefore(value, first);
                return;
            }
            const node = Node.create(value);
            self.first = node;
            self.last = node;
            self.len += 1;
        }

        pub fn append(self: *LinkedList, value: Value) void {
            if (self.last) |last| {
                self.insertAfter(value, last);
                return;
            }
            const node = Node.create(value);
            self.first = node;
            self.last = node;
            self.len += 1;
        }

        pub fn insertBefore(
            self: *LinkedList,
            value: Value,
            existing_node: *Node,
        ) void {
            const node = Node.create(value);
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
            self: *LinkedList,
            value: Value,
            existing_node: *Node,
        ) void {
            const node = Node.create(value);
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
            self: *LinkedList,
            existing_node: *Node,
        ) void {
            defer existing_node.destroy();
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

        pub fn join(self: *LinkedList, other: *LinkedList) void {
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

        pub fn rotateTailToHead(self: *LinkedList) void {
            if (self.len <= 1) return;

            const tail = self.last.?;
            self.last = tail.prev;
            self.last.?.next = null;

            self.first.?.prev = tail;
            tail.prev = null;
            tail.next = self.first;
            self.first = tail;
        }

        pub fn rotateHeadToTail(self: *LinkedList) void {
            if (self.len <= 1) return;

            const head = self.first.?;
            self.first = head.next;
            self.first.?.prev = null;

            self.last.?.next = head;
            head.prev = self.last;
            head.next = null;
            self.last = head;
        }

        pub fn iterator(
            self: *LinkedList,
            direction: Iterator.Direction,
        ) Iterator {
            return .init(self, direction);
        }

        pub fn dupe(self: *LinkedList) *LinkedList {
            var dup_list = LinkedList.create(self.ctx.vtable);

            var it = self.iterator(.forward);
            while (it.next()) |node| {
                const dup_value = self.ctx.dupe(node.value);
                dup_list.append(dup_value);
            }
            return dup_list;
        }

        pub fn search(self: *LinkedList, key: SearchKey) ?*Node {
            var it = self.iterator(.forward);
            while (it.next()) |node| {
                if (self.ctx.eql(key, node.value)) {
                    return node;
                }
            }
            return null;
        }

        pub fn index(self: *LinkedList, idx: i64) ?*Node {
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

        pub fn empty(self: *LinkedList) void {
            var curr = self.first;
            while (curr) |node| {
                curr = node.next;
                self.ctx.free(node.value);
                node.destroy();
            }
            self.len = 0;
            self.first = null;
            self.last = null;
        }

        pub fn release(self: *LinkedList) void {
            self.empty();
            allocator.destroy(self);
        }
    };
}

test "prepend() |insertBefore()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.prepend(1);
    ll.prepend(2);

    try expectEqual(2, ll.len);
    try expectEqual(2, ll.first.?.value);

    ll.insertBefore(3, ll.last.?);
    try expectEqual(ll.first.?.next.?.value, 3);
}

test "append() | insertAfter()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);

    try expectEqual(2, ll.len);
    try expectEqual(2, ll.last.?.value);

    ll.insertAfter(3, ll.first.?);
    try expectEqual(3, ll.first.?.next.?.value);
}

test "removeNode()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

    const node2 = ll.first.?.next.?;
    ll.removeNode(node2);

    try expectEqual(2, ll.len);
    try expectEqual(1, ll.first.?.value);
    try expectEqual(3, ll.last.?.value);
}

test "rotateTailToHead()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

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

test "rotateHeadToTail()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

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

test "iterator(forward|backward) | Iterator.rewind(forward|backward)" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

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

test "dupe()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

    var dup = ll.dupe();
    defer dup.release();

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

test "search()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

    const found = ll.search(3);
    try expect(found != null);
    try expectEqual(3, found.?.value);
    try expectEqual(3, found.?.value);
}

test "index()" {
    var ll: *TestList = TestList.create(TestVtable.vtable);
    defer ll.release();

    ll.append(1);
    ll.append(2);
    ll.append(3);

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

test "join()" {
    var l1: *TestList = TestList.create(TestVtable.vtable);
    defer l1.release();

    var l2: *TestList = TestList.create(TestVtable.vtable);
    defer l2.release();

    l1.append(1);
    l1.append(2);
    l2.append(3);
    l2.append(4);

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

const TestList = List(
    u32,
    u32,
);

const TestVtable = struct {
    const vtable: *const TestList.Vtable = &.{
        .eql = eql,
        .dupe = dupe,
        .free = free,
    };

    fn eql(key: u32, value: u32) bool {
        return key == value;
    }

    fn dupe(value: u32) u32 {
        return value;
    }

    fn free(_: u32) void {}
};

const std = @import("std");
const allocator = @import("allocator.zig");
const testing = std.testing;
const assert = std.debug.assert;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
