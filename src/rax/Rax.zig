const Rax = @This();
tree: art_tree,

pub fn new() Rax {
    var r = Rax{
        .tree = undefined,
    };
    _ = art_tree_init(&r.tree);
    return r;
}

/// Insert the element 's', setting as auxiliary data the pointer 'data'.
/// If the element is already present, the associated data is updated,
/// and old is returned, otherwise the element is inserted and null
/// is returned.
pub fn insert(self: *Rax, s: []const u8, data: *anyopaque) ?*anyopaque {
    return art_insert(&self.tree, s.ptr, @intCast(s.len), data);
}

/// Remove the specified item. Returns old if the item was found and
/// deleted, null otherwise.
pub fn remove(self: *Rax, s: []const u8) ?*anyopaque {
    return art_delete(&self.tree, s.ptr, @intCast(s.len));
}

pub fn free(self: *Rax) void {
    _ = art_tree_destroy(&self.tree);
}

const art = @cImport({
    @cInclude("art.h");
});
const art_tree = art.art_tree;
const art_tree_init = art.art_tree_init;
const art_insert = art.art_insert;
const art_delete = art.art_delete;
const art_tree_destroy = art.art_tree_destroy;
