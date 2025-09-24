pub inline fn memcpy(noalias dest: anytype, noalias src: anytype, len: usize) void {
    @memcpy(dest[0..len], src[0..len]);
}

pub inline fn memmove(dest: anytype, src: anytype, len: usize) void {
    @memmove(dest[0..len], src[0..len]);
}

pub inline fn memset(dest: anytype, elem: anytype, len: usize) void {
    @memset(dest[0..len], elem);
}
