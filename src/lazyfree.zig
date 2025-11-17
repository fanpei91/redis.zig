const LAZYFREE_THRESHOLD = 64;

/// Delete a key, value, and associated expiration entry if any, from the DB.
/// If there are enough allocations to free the value object may be put into
/// a lazy free list instead of being freed synchronously. The lazy free list
/// will be reclaimed in a different bio.zig thread.
pub fn asyncDelete(db: *Database, key: *Object) bool {
    // Deleting an entry from the expires dict will not free the sds of
    // the key, because it is shared with the main dictionary.
    if (db.expires.size() > 0) {
        _ = db.expires.delete(key.v.ptr);
    }

    // If the value is composed of a few allocations, to free in a lazy way
    // is actually just slower... So under a certain limit we just free
    // the object synchronously.
    const entry = db.dict.unlink(key.v.ptr);
    if (entry) |ent| {
        const val: *Object = @ptrCast(@alignCast(ent.v.val.?));
        const free_effort = getFreeEffort(val);

        // If releasing the object is too much work, do it in the background
        // by adding the object to the lazy free list.
        // Note that if the object is shared, to reclaim it now it is not
        // possible. This rarely happens, however sometimes the implementation
        // of parts of the Redis core may call incrRefCount() to protect
        // objects, and then call Database.delete(). In this case we'll fall
        // through and reach the Dict.freeUnlinkedEntry() call, that will be
        // equivalent to just calling decrRefCount().
        if (free_effort > LAZYFREE_THRESHOLD and val.refcount == 1) {
            bio.createBackgroundJob(bio.Job.Type.lazyFree, val, null, null);
            db.dict.setVal(ent, null);
        }

        // Release the key-val pair, or just the key if we set the val
        // field to null in order to lazy free it later.
        db.dict.freeUnlinkedEntry(ent);
        return true;
    }
    return false;
}

/// Return the amount of work needed in order to free an object.
/// The return value is not always the actual number of allocations the
/// object is compoesd of, but a number proportional to it.
///
/// For strings the function always returns 1.
///
/// For aggregated objects represented by hash tables or other data structures
/// the function just returns the number of elements the object is composed of.
///
/// Objects composed of single allocations are always reported as having a
/// single item even if they are actually logical composed of multiple
/// elements.
///
/// For lists the function returns the number of elements in the quicklist
/// representing the list.
fn getFreeEffort(obj: *Object) u64 {
    const ptr = obj.v.ptr;

    if (obj.type == .list) {
        const ql: *Quicklist = @ptrCast(@alignCast(ptr));
        return ql.len;
    }

    if (obj.type == .set and obj.encoding == .ht) {
        const dict: *Dict = @ptrCast(@alignCast(ptr));
        return dict.size();
    }

    if (obj.type == .zset and obj.encoding == .skiplist) {
        const sz: *Zset = @ptrCast(@alignCast(ptr));
        return sz.zsl.length;
    }

    if (obj.type == .hash and obj.encoding == .ht) {
        const dict: *Dict = @ptrCast(@alignCast(ptr));
        return dict.size();
    }

    // TODO: STREAM

    // Everything else is a single allocation.
    return 1;
}

pub fn freeObjectFromBioThread(obj: *Object) void {
    obj.decrRefCount();
}

pub fn freeDatabaseFromBioThread(h1: *Dict, h2: *Dict) void {
    h1.destroy();
    h2.destroy();
}

const Database = @import("db.zig").Database;
const Object = @import("Object.zig");
const Quicklist = @import("QuickList.zig");
const Dict = @import("Dict.zig");
const Zset = @import("t_zset.zig").Zset;
const bio = @import("bio.zig");
