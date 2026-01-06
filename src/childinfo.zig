/// Open a child-parent channel used in order to move information about the
/// RDB / AOF saving process from the child to the parent (for instance
/// the amount of copy on write memory used)
pub fn openPipe() void {
    // TODO:
}

/// Close the pipes opened with openPipe().
pub fn closePipe() void {
    // TODO:
}

/// Send COW data to parent. The child should call this function after populating
/// the corresponding fields it want to sent (according to the process type).
pub fn send(ptype: i32) void {
    _ = ptype;
    // TODO:
}

/// Receive COW data from parent.
pub fn receive() void {
    // TODO:
}
