const LAZYFREE_THRESHOLD = 64;

/// Delete a key, value, and associated expiration entry if any, from the DB.
/// If there are enough allocations to free the value object may be put into
/// a lazy free list instead of being freed synchronously. The lazy free list
/// will be reclaimed in a different bio.zig thread.
pub fn asyncDelete(db: *Database, key: *Object) bool {
    const skey = sds.cast(key.v.ptr);
    // Deleting an entry from the expires dict will not free the sds of
    // the key, because it is shared with the main dictionary.
    if (db.expires.size() > 0) {
        _ = db.expires.delete(skey);
    }

    // If the value is composed of a few allocations, to free in a lazy way
    // is actually just slower... So under a certain limit we just free
    // the object synchronously.
    const entry = db.dict.unlink(skey);
    if (entry) |ent| {
        const val = ent.val.?;
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
        const ql: *Quicklist = .cast(ptr);
        return ql.len;
    }

    if (obj.type == .set and obj.encoding == .ht) {
        const h: *Set.Hash = .cast(ptr);
        return h.size();
    }

    if (obj.type == .zset and obj.encoding == .skiplist) {
        const sz: *SkipListSet = .cast(ptr);
        return sz.sl.length;
    }

    if (obj.type == .hash and obj.encoding == .ht) {
        const h: *Hash.Map = .cast(ptr);
        return h.size();
    }

    // TODO: STREAM

    // Everything else is a single allocation.
    return 1;
}

pub fn freeObjectFromBioThread(obj: *Object) void {
    obj.decrRefCount();
}

pub fn freeDatabaseFromBioThread(
    db1: *Database.Hash.Map,
    db2: *Database.Hash.Map,
) void {
    db1.destroy();
    db2.destroy();
}

const Database = @import("db.zig").Database;
const Object = @import("Object.zig");
const Quicklist = @import("QuickList.zig");
const SkipListSet = @import("t_zset.zig").SkipListSet;
const Set = @import("t_set.zig").Set;
const Hash = @import("t_hash.zig").Hash;
const bio = @import("bio.zig");
const sds = @import("sds.zig");
