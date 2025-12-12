// Every stream item inside the listpack, has a flags field that is used to
// mark the entry as deleted, or having the same field as the "master"
// entry at the start of the listpack> */
const ITEM_FLAG_NONE = 0; // No special flags.
const ITEM_FLAG_DELETED = (1 << 0); // Entry is delted. Skip it.
const ITEM_FLAG_SAMEFIELDS = (1 << 1); // Same fields as master entry.

// Flags for streamLookupConsumer
pub const SLC_NONE = 0;
/// Do not create the consumer if it doesn't exist
pub const SLC_NOCREAT = (1 << 0);
/// Do not update consumer's seen-time
pub const SLC_NOREFRESH = (1 << 1);

/// Don't let listpacks grow too big, even if the user config allows it.
/// Doing so can lead to an overflow (trying to store more than 32bit length
/// into the listpack header), or actually an assertion since LispPask.insert()
/// will return NULL.
pub const LISTPACK_MAX_SIZE = (1 << 30);

/// Stream item ID: a 128 bit number composed of a milliseconds time and
/// a sequence counter. IDs generated in the same millisecond (or in a past
/// millisecond if the clock jumped backward) will use the millisecond time
/// of the latest generated ID and an incremented sequence.
pub const Id = struct {
    /// Unix time in milliseconds.
    ms: u64,
    /// Sequence number.
    seq: u64,

    /// Convert the specified stream entry ID as a 128 bit big endian number, so
    /// that the IDs can be sorted lexicographically.
    pub fn encode(self: *const Id, buf: []u8) void {
        assert(buf.len >= @sizeOf(Id));
        writeInt(u64, buf[0..8], self.ms, .big);
        writeInt(u64, buf[8..16], self.seq, .big);
    }

    /// This is the reverse of decode(): the decoded ID will be stored
    /// in the 'self' structure passed by reference. The buffer 'buf'
    /// must point to a 128 bit big-endian encoded ID.
    pub fn decode(self: *Id, buf: []u8) void {
        assert(buf.len >= @sizeOf(Id));
        self.ms = readInt(u64, buf[0..8], .big);
        self.seq = readInt(u64, buf[8..16], .big);
    }

    /// Generate the next stream item ID given the previous one. If the current
    /// milliseconds Unix time is greater than the previous one, just use this
    /// as time part and start with sequence part of zero. Otherwise we use the
    /// previous time (and never go backward) and increment the sequence.
    pub fn nextId(self: *const Id) Id {
        var new_id: Id = undefined;
        const ms = std.time.milliTimestamp();
        if (ms > self.ms) {
            new_id.ms = @intCast(ms);
            new_id.seq = 0;
        } else {
            new_id = self.*;
            new_id.incr();
        }
        return new_id;
    }

    /// Set 'id' to be its successor Id.
    pub fn incr(self: *Id) void {
        const max = std.math.maxInt(u64);
        if (self.seq == max) {
            if (self.ms == max) {
                self.ms = 0;
                self.seq = 0;
            } else {
                self.ms += 1;
                self.seq = 0;
            }
        } else {
            self.seq += 1;
        }
    }

    /// Compare two stream IDs.
    pub fn compare(self: *const Id, other: *const Id) std.math.Order {
        if (self.ms > other.ms) return .gt;
        if (self.ms < other.ms) return .lt;
        // The ms part is the same. Check the sequence part.
        if (self.seq > other.seq) return .gt;
        if (self.seq < other.seq) return .lt;
        // Everything is the same: IDs are equal.
        return .eq;
    }
};

/// Pending (yet not acknowledged) message in a consumer group.
pub const NACK = struct {
    /// Last unix timestamp in ms this message was delivered.
    delivery_time: i64,
    /// Number of times this message was delivered.
    delivery_count: u64,
    /// The consumer this message was delivered to
    /// in the last delivery.
    consumer: *Consumer,

    /// Create a NACK entry setting the delivery count to 1 and the delivery
    /// time to the current time. The NACK consumer will be set to the one
    /// specified as argument of the function.
    pub fn create(consumer: *Consumer) *NACK {
        const nack = allocator.create(NACK);
        nack.delivery_time = std.time.milliTimestamp();
        nack.delivery_count = 1;
        nack.consumer = consumer;
        return nack;
    }

    /// Destroy a NACK entry.
    pub fn destroy(self: *NACK) void {
        allocator.destroy(self);
    }
};

/// A specific consumer in a consumer group.
pub const Consumer = struct {
    /// Last unix timestamp in ms this consumer was active.
    seen_time: i64,
    /// Consumer name. This is how the consumer
    /// will be identified in the consumer group
    /// protocol. Case sensitive.
    name: sds.String,
    /// Consumer specific pending entries list: all
    /// the pending messages delivered to this
    /// consumer not yet acknowledged. Keys are
    /// big endian message IDs, while values are
    /// the same NACK structure referenced
    /// in the "pel" of the conumser group structure
    /// itself, so the value is shared.
    pel: [*c]raxlib.rax,

    /// Create consumer with the specified name.
    pub fn create(name: sds.String) *Consumer {
        const consumer = allocator.create(Consumer);
        consumer.name = sds.dupe(allocator.child, name);
        consumer.pel = raxlib.raxNew();
        return consumer;
    }

    /// Destroy a consumer and associated data structures. Note that this function
    /// will not reassign the pending messages associated with this consumer
    /// nor will delete them from the stream, so when this function is called
    /// to delete a consumer, and not when the whole stream is destroyed, the caller
    /// should do some work before.
    pub fn destroy(self: *Consumer) void {
        // No value free callback: the PEL entries are shared
        // between the consumer and the main stream PEL.
        raxlib.raxFree(self.pel);

        sds.free(allocator.child, self.name);
        allocator.destroy(self);
    }
};

/// Consumer group.
pub const CG = struct {
    /// Last delivered (not acknowledged) ID for this
    /// group. Consumers that will just ask for more
    /// messages will served with IDs > than this.
    last_id: Id,
    /// Pending entries list. This is a radix tree that
    /// has every message delivered to consumers (without
    /// the NOACK option) that was yet not acknowledged
    /// as processed. The key of the radix tree is the
    /// ID as a 64 bit big endian number, while the
    /// associated value is a NACK structure.
    pel: [*c]raxlib.rax,
    /// A radix tree representing the consumers by name
    /// and their associated representation in the form
    /// of Consumer structures.
    consumers: [*c]raxlib.rax,

    /// Create Consumer Group with the specified id.
    pub fn create(id: Id) *CG {
        const cg = allocator.create(CG);
        cg.pel = raxlib.raxNew();
        cg.consumers = raxlib.raxNew();
        cg.last_id = id;
        return cg;
    }

    /// Lookup the consumer with the specified name in the group: if the
    /// consumer does not exist it is automatically created as a side effect
    /// of calling this function, otherwise its last seen time is updated and
    /// the existing consumer reference returned.
    pub fn lookupConsumer(
        self: *CG,
        name: sds.String,
        flags: i32,
    ) ?*Consumer {
        const creating = flags & SLC_NOCREAT == 0;
        const refreshing = flags & SLC_NOREFRESH == 0;
        var consumer: *Consumer = undefined;
        const value = raxlib.raxFind(self.consumers, name, sds.getLen(name));
        if (value == raxlib.raxNotFound) {
            if (!creating) return null;
            consumer = Consumer.create(name);
            _ = raxlib.raxInsert(
                self.consumers,
                name,
                sds.getLen(name),
                consumer,
                null,
            );
        } else {
            consumer = @ptrCast(@alignCast(value.?));
        }
        if (refreshing) {
            consumer.seen_time = std.time.milliTimestamp();
        }
        return consumer;
    }

    /// Free a consumer group and all its associated data.
    pub fn destroy(self: *CG) void {
        raxlib.raxFreeWithCallback(self.pel, freeNACKCallback);
        raxlib.raxFreeWithCallback(self.consumers, freeConsumerCallback);
        allocator.destroy(self);
    }

    fn freeNACKCallback(ptr: ?*anyopaque) callconv(.c) void {
        const nack: *NACK = @ptrCast(@alignCast(ptr.?));
        nack.destroy();
    }

    fn freeConsumerCallback(ptr: ?*anyopaque) callconv(.c) void {
        const consumer: *Consumer = @ptrCast(@alignCast(ptr.?));
        consumer.destroy();
    }
};

/// We define an iterator to iterate stream items in an abstract way, without
/// caring about the radix tree + listpack representation. Technically speaking
/// the iterator is only used inside replyWithRange(), so could just
/// be implemented inside the function, but practically there is the AOF
/// rewriting code that also needs to iterate the stream to emit the XADD
/// commands.
pub const Iterator = struct {
    /// The stream we are iterating.
    stream: *Stream,
    /// ID of the master entry at listpack head.
    master_id: Id,
    /// Master entries # of fields.
    master_fields_count: u64,
    /// Master entries start in listpack.
    master_fields_start: [*]u8,
    /// Master field to emit next.
    master_fields_ptr: [*]u8,
    /// Flags of entry we are emitting.
    entry_flags: i32,
    /// True if iterating end to start (reverse).
    rev: bool,
    /// Start key as 128 bit big endian.
    start_key: [2]u64,
    /// End key as 128 bit big endian.
    end_key: [2]u64,
    /// Rax iterator.
    ri: raxlib.raxIterator,
    /// Current listpack.
    lp: ?*ListPack,
    /// Current listpack cursor.
    lp_ele: ?[*]u8,
    /// Current entry flags pointer.
    lp_flags: ?[*]u8,
    /// Buffers used to hold the string of ListPack.get() when the element is
    /// integer encoded, so that there is no string representation of the
    /// element inside the listpack itself.
    field_buf: [ListPack.INTBUF_SIZE]u8,
    value_buf: [ListPack.INTBUF_SIZE]u8,

    /// Initialize the stream iterator, so that we can call iterating functions
    /// to get the next items. This requires a corresponding stop() at the end.
    /// The 'rev' parameter controls the direction. If it's false the iteration
    /// is from the start to the end element (inclusive), otherwise if rev is
    /// true, the iteration is reversed.
    pub fn start(
        self: *Iterator,
        stream: *Stream,
        start_id: ?*Id,
        end_id: ?*Id,
        rev: bool,
    ) void {
        // Intialize the iterator and translates the iteration start/stop
        // elements into a 128 big big-endian number.
        if (start_id) |id| {
            id.encode(sliceAsBytes(&self.start_key));
        } else {
            self.start_key[0] = 0;
            self.start_key[1] = 0;
        }
        if (end_id) |id| {
            id.encode(sliceAsBytes(&self.end_key));
        } else {
            self.end_key[0] = std.math.maxInt(u64);
            self.end_key[1] = std.math.maxInt(u64);
        }

        // Seek the correct node in the radix tree.
        raxlib.raxStart(&self.ri, stream.rax);
        if (!rev) {
            if (start_id != null and (start_id.?.ms != 0 or start_id.?.seq != 0)) {
                const start_key: []u8 = sliceAsBytes(&self.start_key);
                _ = raxlib.raxSeek(&self.ri, "<=", start_key.ptr, start_key.len);
                if (raxlib.raxEOF(&self.ri) != 0) {
                    _ = raxlib.raxSeek(&self.ri, "^", null, 0);
                }
            } else {
                _ = raxlib.raxSeek(&self.ri, "^", null, 0);
            }
        } else {
            if (end_id != null and (end_id.?.ms != 0 or end_id.?.seq != 0)) {
                const end_key: []u8 = sliceAsBytes(&self.end_key);
                _ = raxlib.raxSeek(&self.ri, "<=", end_key.ptr, end_key.len);
                if (raxlib.raxEOF(&self.ri) != 0) {
                    _ = raxlib.raxSeek(&self.ri, "$", null, 0);
                }
            } else {
                _ = raxlib.raxSeek(&self.ri, "$", null, 0);
            }
        }
        self.stream = stream;
        self.lp = null; // There is no current listpack right now.
        self.lp_ele = null; //  Current listpack cursor.
        self.rev = rev; // Direction, if true reversed, from end to start.
    }

    /// Return TRUE and store the current item ID at 'id' if there are still
    /// elements within the iteration range, otherwise return FALSE in order to
    /// signal the iteration terminated.
    pub fn getId(self: *Iterator, id: *Id, numfields: *i64) bool {
        // Will stop when element > stop_key or end of radix tree.
        while (true) {
            // If the current listpack is set to NULL, this is the start of the
            // iteration or the previous listpack was completely iterated.
            // Go to the next node.
            if (self.lp == null or self.lp_ele == null) {
                if (!self.rev and raxlib.raxNext(&self.ri) == 0) {
                    return false;
                } else if (self.rev and raxlib.raxPrev(&self.ri) == 0) {
                    return false;
                }
                assert(self.ri.key_len == @sizeOf(Id));
                // Get the master ID.
                self.master_id.decode(self.ri.key[0..self.ri.key_len]);
                // Get the master fields count.
                self.lp = @ptrCast(@alignCast(self.ri.data.?));
                self.lp_ele = self.lp.?.first(); // Seek items count
                self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek deleted count.
                self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek num fields.
                self.master_fields_count = @intCast(ListPack.getInteger(self.lp_ele.?));
                self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek first field.
                self.master_fields_start = self.lp_ele.?;
                // We are now pointing to the first field of the master entry.
                // We need to seek either the first or the last entry depending
                // on the direction of the iteration.
                if (!self.rev) {
                    for (0..self.master_fields_count) |_| {
                        self.lp_ele = self.lp.?.next(self.lp_ele.?);
                    }
                } else {
                    // If we are iterating in reverse direction, just seek the
                    // last part of the last entry in the listpack (that is, the
                    // fields count).
                    self.lp_ele = self.lp.?.last();
                }
            } else if (self.rev) {
                // If we are itereating in the reverse order, and this is not
                // the first entry emitted for this listpack, then we already
                // emitted the current entry, and have to go back to the previous
                // one.
                var lp_count = ListPack.getInteger(self.lp_ele.?);
                while (lp_count > 0) : (lp_count -= 1) {
                    self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                }
                // Seek lp-count of prev entry.
                self.lp_ele = self.lp.?.prev(self.lp_ele.?);
            }

            // For every radix tree node, iterate the corresponding listpack,
            // returning elements when they are within range.
            while (true) {
                if (!self.rev) {
                    // If we are going forward, skip the previous entry
                    // lp-count field (or in case of the master entry, the zero
                    // term field)
                    self.lp_ele = self.lp.?.next(self.lp_ele.?);
                    if (self.lp_ele == null) break;
                } else {
                    // If we are going backward, read the number of elements this
                    // entry is composed of, and jump backward N times to seek
                    // its start.
                    var lp_count = ListPack.getInteger(self.lp_ele.?);
                    if (lp_count == 0) {
                        // We reached the master entry.
                        self.lp = null;
                        self.lp_ele = null;
                        break;
                    }
                    while (lp_count > 0) : (lp_count -= 1) {
                        self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                    }
                }

                // Get the flags entry.
                self.lp_flags = self.lp_ele;
                const flags: i32 = @intCast(ListPack.getInteger(self.lp_flags.?));
                self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek ID.

                // Get the ID: it is encoded as difference between the master
                // ID and this entry ID.
                id.* = self.master_id;
                id.ms +%= @as(u64, @intCast(ListPack.getInteger(self.lp_ele.?)));
                self.lp_ele = self.lp.?.next(self.lp_ele.?);
                id.seq +%= @as(u64, @intCast(ListPack.getInteger(self.lp_ele.?)));
                self.lp_ele = self.lp.?.next(self.lp_ele.?);
                var buf: [@sizeOf(Id)]u8 = undefined;
                id.encode(&buf);

                // The number of entries is here or not depending on the
                // flags.
                if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                    numfields.* = @intCast(self.master_fields_count);
                } else {
                    numfields.* = ListPack.getInteger(self.lp_ele.?);
                    self.lp_ele = self.lp.?.next(self.lp_ele.?);
                }

                // If current >= start, and the entry is not marked as
                // deleted, emit it.
                const start_key: []u8 = sliceAsBytes(&self.start_key);
                const end_key: []u8 = sliceAsBytes(&self.end_key);
                if (!self.rev) {
                    if (memcmp(&buf, start_key).compare(.gte) and
                        (flags & ITEM_FLAG_DELETED == 0))
                    {
                        if (memcmp(&buf, end_key) == .gt) {
                            // We are already out of range.
                            return false;
                        }
                        self.entry_flags = flags;
                        if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                            self.master_fields_ptr = self.master_fields_start;
                        }
                        // Valid item returned.
                        return true;
                    }
                } else {
                    if (memcmp(&buf, end_key).compare(.lte) and
                        (flags & ITEM_FLAG_DELETED == 0))
                    {
                        if (memcmp(&buf, start_key) == .lt) {
                            // We are already out of range.
                            return false;
                        }
                        self.entry_flags = flags;
                        if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                            self.master_fields_ptr = self.master_fields_start;
                        }
                        return true;
                    }
                }

                // If we do not emit, we have to discard if we are going
                // forward, or seek the previous entry if we are going
                // backward.
                if (!self.rev) {
                    const to_discard = if (flags & ITEM_FLAG_SAMEFIELDS != 0)
                        numfields.*
                    else
                        numfields.* * 2;
                    for (0..@as(u64, @intCast(to_discard))) |_| {
                        self.lp_ele = self.lp.?.next(self.lp_ele.?);
                    }
                } else {
                    // flag + id ms + id seq + one more to
                    // go back to the previous entry "count"
                    // field.
                    var prev_items: usize = 4;
                    // If the entry was not flagged SAMEFIELD we also read the
                    // number of fields, so go back one more.
                    if (flags & ITEM_FLAG_SAMEFIELDS == 0) prev_items += 1;
                    while (prev_items > 0) : (prev_items -= 1) {
                        self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                    }
                }
            }

            // End of listpack reached. Try the next/prev radix tree node.
        }
    }

    /// Get the field and value of the current item we are iterating. This should
    /// be called immediately after Iterator.getID(), and for each field according
    /// to the number of fields returned by Iterator.getID().
    /// The function populates the field and value pointers, that are valid until
    /// the next iterator call, assuming no one touches the stream meanwhile.
    pub fn getField(self: *Iterator, field_ptr: *[]u8, value_ptr: *[]u8) void {
        var field_len: i64 = undefined;
        var value_len: i64 = undefined;
        if (self.entry_flags & ITEM_FLAG_SAMEFIELDS != 0) {
            const ptr = ListPack.get(
                self.master_fields_ptr,
                &field_len,
                &self.field_buf,
            ).?;
            field_ptr.* = ptr[0..@intCast(field_len)];
            self.master_fields_ptr = self.lp.?.next(self.master_fields_ptr).?;
        } else {
            const ptr = ListPack.get(
                self.lp_ele.?,
                &field_len,
                &self.field_buf,
            ).?;
            field_ptr.* = ptr[0..@intCast(field_len)];
            self.lp_ele = self.lp.?.next(self.lp_ele.?);
        }
        const ptr = ListPack.get(
            self.lp_ele.?,
            &value_len,
            &self.value_buf,
        ).?;
        value_ptr.* = ptr[0..@intCast(value_len)];
        self.lp_ele = self.lp.?.next(self.lp_ele.?);
    }

    /// Stop the stream iterator. The only cleanup we need is to free the rax
    /// itereator, since the stream iterator itself is supposed to be stack
    /// allocated.
    pub fn stop(self: *Iterator) void {
        raxlib.raxStop(&self.ri);
    }

    /// Remove the current entry from the stream: can be called after the
    /// getId() API or after any detField() call, however we need to iterate
    /// a valid entry while calling this function. Moreover the function
    /// requires the entry ID we are currently iterating, that was previously
    /// returned by getId().
    ///
    /// Note that after calling this function, next calls to getField() can't
    /// be performed: the entry is now deleted. Instead the iterator will
    /// automatically re-seek to the next entry, so the caller should continue
    /// with getId().
    fn removeEntry(self: *Iterator, current: *Id) void {
        var lp = self.lp.?;

        // We do not really delete the entry here. Instead we mark it as
        // deleted flagging it, and also incrementing the count of the
        // deleted entries in the listpack header.
        //
        // We start flagging:
        var flags = ListPack.getInteger(self.lp_flags.?);
        flags |= ITEM_FLAG_DELETED;
        lp = lp.replaceInteger(&self.lp_flags, flags).?;

        // Change the valid/deleted entries count in the master entry.
        var p = lp.first();
        var aux = ListPack.getInteger(p.?);
        if (aux == 1) {
            // If this is the last element in the listpack, we can remove the whole
            // node.
            lp.free();
            _ = raxlib.raxRemove(
                self.stream.rax,
                self.ri.key,
                self.ri.key_len,
                null,
            );
        } else {
            // In the base case we alter the counters of valid/deleted entries.
            lp = lp.replaceInteger(&p, aux - 1).?;
            p = lp.next(p.?); // Seek deleted field.
            aux = ListPack.getInteger(p.?);
            lp = lp.replaceInteger(&p, aux + 1).?;

            // Update the listpack with the new pointer.
            if (self.lp != lp) {
                _ = raxlib.raxInsert(
                    self.stream.rax,
                    self.ri.key,
                    self.ri.key_len,
                    lp,
                    null,
                );
            }
        }

        // Update the number of entries counter.
        self.stream.length -= 1;

        // Re-seek the iterator to fix the now messed up state.
        var start_id: Id = undefined;
        var end_id: Id = undefined;
        if (self.rev) {
            start_id.decode(sliceAsBytes(&self.start_key));
            end_id = current.*;
        } else {
            start_id = current.*;
            end_id.decode(sliceAsBytes(&self.end_key));
        }
        self.stop();
        self.start(self.stream, &start_id, &end_id, self.rev);
    }
};

const Stream = @This();
/// The radix tree holding the stream.
rax: [*c]raxlib.rax,
/// Number of elements inside this stream.
length: u64,
/// Zero if there are yet no items.
last_id: Id,
/// Consumer groups dictionary: name -> CG
cgroups: ?[*c]raxlib.rax,

pub fn create() *Stream {
    const s = allocator.create(Stream);
    s.rax = raxlib.raxNew();
    s.length = 0;
    s.last_id.ms = 0;
    s.last_id.seq = 0;
    s.cgroups = null; // Created on demand to save memory when not used.
    return s;
}

/// Adds a new item into the stream 's' having the specified number of
/// field-value pairs as specified in 'numfields' and stored into 'argv'.
/// Returns the new entry ID populating the 'added_id' structure.
///
/// If 'use_id' is not NULL, the ID is not auto-generated by the function,
/// but instead the passed ID is uesd to add the new entry. In this case
/// adding the entry may fail as specified later in this comment.
///
/// 1. If an ID was given via 'use_id', but adding it failed since the
///    current top ID is greater or equal, return error.DOM.
/// 2. If a size of a single element or the sum of the elements is too big to
///    be stored into the stream. return RANGE.
pub fn append(
    self: *Stream,
    argv: []*Object,
    numfields: usize,
    added_id: ?*Stream.Id,
    use_id: ?*Stream.Id,
) error{ DOM, RANGE }!void {
    const id: Stream.Id = blk: {
        if (use_id) |uid| {
            break :blk uid.*;
        }
        break :blk self.last_id.nextId();
    };

    // Check that the new ID is greater than the last entry ID
    // or return an error. Automatically generated IDs might
    // overflow (and wrap-around) when incrementing the sequence
    // part.
    if (id.compare(&self.last_id) != .gt) {
        return error.DOM;
    }

    // Avoid overflow when trying to add an element to the stream (listpack
    // can only host up to 32bit length sttrings, and also a total listpack size
    // can't be bigger than 32bit length.
    var totelelen: usize = 0;
    for (argv) |field| {
        totelelen += sds.getLen(sds.cast(field.v.ptr));
    }
    if (totelelen > Stream.LISTPACK_MAX_SIZE) {
        return error.RANGE;
    }

    // Add the new entry.
    var ri: raxlib.raxIterator = undefined;
    raxlib.raxStart(&ri, self.rax);
    defer raxlib.raxStop(&ri);
    _ = raxlib.raxSeek(&ri, "$", null, 0);

    var lp_bytes: usize = 0; // Total bytes in the tail listpack.
    var lp: ?*ListPack = null;

    // Get a reference to the tail node listpack.
    if (raxlib.raxNext(&ri) != 0) {
        lp = @ptrCast(@alignCast(ri.data.?));
        lp_bytes = lp.?.bytes.get();
    }

    // We have to add the key into the radix tree in lexicographic order,
    // to do so we consider the ID as a single 128 bit number written in
    // big endian, so that the most significant bytes are the first ones.
    var rax_key: [@sizeOf(Id)]u8 = undefined;
    var master_id: Stream.Id = undefined;

    // Create a new listpack and radix tree node if needed. Note that when
    // a new listpack is created, we populate it with a "master entry". This
    // is just a set of fields that is taken as references in order to compress
    // the stream entries that we'll add inside the listpack.
    //
    // Note that while we use the first added entry fields to create
    // the master entry, the first added entry is NOT represented in the master
    // entry, which is a stand alone object. But of course, the first entry
    // will compress well because it's used as reference.
    //
    // The master entry is composed like in the following example:
    //
    // +-------+---------+------------+---------+--/--+---------+---------+-+
    // | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
    // +-------+---------+------------+---------+--/--+---------+---------+-+
    //
    // count and deleted just represent respectively the total number of
    // entries inside the listpack that are valid, and marked as deleted
    // (delted flag in the entry flags set). So the total number of items
    // actually inside the listpack (both deleted and not) is count+deleted.
    //
    // The real entries will be encoded with an ID that is just the
    // millisecond and sequence difference compared to the key stored at
    // the radix tree node containing the listpack (delta encoding), and
    // if the fields of the entry are the same as the master enty fields, the
    // entry flags will specify this fact and the entry fields and number
    // of fields will be omitted (see later in the code of this function).
    //
    // The "0" entry at the end is the same as the 'lp-count' entry in the
    // regular stream entries (see below), and marks the fact that there are
    // no more entries, when we scan the stream from right to left.

    // First of all, check if we can append to the current macro node or
    // if we need to switch to the next one. 'lp' will be set to NULL if
    // the current node is full.
    if (lp != null) {
        var node_max_bytes = server.stream_node_max_bytes;
        if (node_max_bytes == 0 or node_max_bytes > Stream.LISTPACK_MAX_SIZE) {
            node_max_bytes = Stream.LISTPACK_MAX_SIZE;
        }
        if (lp_bytes + totelelen >= node_max_bytes) {
            lp = null;
        } else if (server.stream_node_max_entries != 0) {
            const count = ListPack.getInteger(lp.?.first().?);
            if (count >= server.stream_node_max_entries) {
                lp = null;
            }
        }
    }

    var flags: i32 = ITEM_FLAG_NONE;
    if (lp == null or lp_bytes >= server.stream_node_max_bytes) {
        master_id = id;
        // Create the listpack having the master entry ID and fields.
        id.encode(&rax_key);
        lp = ListPack.new();
        lp = lp.?.appendInteger(1); // One item, the one we are adding.
        lp = lp.?.appendInteger(0); // Zero deleted so far.
        lp = lp.?.appendInteger(@intCast(numfields));
        for (0..numfields) |i| {
            const field = sds.castBytes(argv[i * 2].v.ptr);
            lp = lp.?.append(field);
        }
        lp = lp.?.appendInteger(0); // Master entry zero terminator.
        _ = raxlib.raxInsert(self.rax, &rax_key, rax_key.len, lp, null);
        // The first entry we insert, has obviously the same fields of the
        // master entry.
        flags |= ITEM_FLAG_SAMEFIELDS;
    } else {
        assert(ri.key_len == rax_key.len);
        memcpy(&rax_key, ri.key, ri.key_len);

        // Read the master ID from the radix tree key.
        master_id.decode(&rax_key);
        var lp_ele = lp.?.first();

        // Update count and skip the deleted fields.
        const count = ListPack.getInteger(lp_ele.?);
        lp = lp.?.replaceInteger(&lp_ele, count + 1);
        lp_ele = lp.?.next(lp_ele.?); // seek deleted.
        lp_ele = lp.?.next(lp_ele.?); // seek master entry num fields.

        // Check if the entry we are adding, have the same fields
        // as the master entry.
        const master_fields_count = ListPack.getInteger(lp_ele.?);
        lp_ele = lp.?.next(lp_ele.?);
        if (numfields == master_fields_count) {
            var i: usize = 0;
            while (i < master_fields_count) : (i += 1) {
                var ele_len: i64 = undefined;
                var buf: [ListPack.INTBUF_SIZE]u8 = undefined;
                const field = sds.castBytes(argv[i * 2].v.ptr);
                const e = ListPack.get(lp_ele.?, &ele_len, &buf).?;
                if (ele_len != field.len or
                    memcmp(e[0..@intCast(ele_len)], field) != .eq)
                {
                    break;
                }
                lp_ele = lp.?.next(lp_ele.?);
            }
            // All fields are the same! We can compress the field names
            // setting a single bit in the flags.
            if (i == master_fields_count) {
                flags |= ITEM_FLAG_SAMEFIELDS;
            }
        }
    }

    // Populate the listpack with the new entry. We use the following
    // encoding:
    //
    // +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
    // |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
    // +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
    //
    // However if the SAMEFIELD flag is set, we have just to populate
    // the entry with the values, so it becomes:
    //
    // +-----+--------+-------+-/-+-------+--------+
    // |flags|entry-id|value-1|...|value-N|lp-count|
    // +-----+--------+-------+-/-+-------+--------+
    //
    // The entry-id field is actually two separated fields: the ms
    // and seq difference compared to the master entry.
    //
    // The lp-count field is a number that states the number of listpack pieces
    // that compose the entry, so that it's possible to travel the entry
    // in reverse order: we can just start from the end of the listpack, read
    // the entry, and jump back N times to seek the "flags" field to read
    // the stream full entry.
    lp = lp.?.appendInteger(flags);
    lp = lp.?.appendInteger(@intCast(id.ms - master_id.ms));
    lp = lp.?.appendInteger(@intCast(id.seq - master_id.seq));
    if (flags & ITEM_FLAG_SAMEFIELDS == 0) {
        lp = lp.?.appendInteger(@intCast(numfields));
    }
    for (0..numfields) |i| {
        const field = sds.castBytes(argv[i * 2].v.ptr);
        const value = sds.castBytes(argv[i * 2 + 1].v.ptr);
        if (flags & ITEM_FLAG_SAMEFIELDS == 0) {
            lp = lp.?.append(field);
        }
        lp = lp.?.append(value);
    }
    // Compute and store the lp-count field.
    var lp_count = numfields;
    // Add the 3 fixed fields flags + ms-diff + seq-diff.
    lp_count += 3;
    if (flags & ITEM_FLAG_SAMEFIELDS == 0) {
        // If the item is not compressed, it also has the fields other than
        // the values, and an additional num-fileds field.
        lp_count += numfields + 1;
    }
    lp = lp.?.appendInteger(@intCast(lp_count));

    // Insert back into the tree in order to update the listpack pointer.
    if (@as(?*ListPack, @ptrCast(@alignCast(ri.data))) != lp) {
        _ = raxlib.raxInsert(self.rax, &rax_key, rax_key.len, lp, null);
    }
    self.length += 1;
    self.last_id = id;
    if (added_id) |aid| aid.* = id;
}

/// Delete the specified item ID from the stream, returning TRUE if the item
/// was deleted FALSE otherwise (if it does not exist).
pub fn delete(self: *Stream, id: *Id) bool {
    var deleted = false;

    var it: Iterator = undefined;
    it.start(self, id, id, false);
    defer it.stop();

    var myid: Id = undefined;
    var numfields: i64 = undefined;
    if (it.getId(&myid, &numfields)) {
        it.removeEntry(&myid);
        deleted = true;
    }
    return deleted;
}

/// Trim the stream to have no more than maxlen elements, and return the
/// number of elements removed from the stream. The 'approx' option, if true,
/// specifies that the trimming must be performed in a approximated way in
/// order to maximize performances. This means that the stream may contain
/// more elements than 'maxlen', and elements are only removed if we can remove
/// a *whole* node of the radix tree. The elements are removed from the head
/// of the stream (older elements).
///
/// The function may return zero if:
///
/// 1) The stream is already shorter or equal to the specified max length.
/// 2) The 'approx' option is true and the head node had not enough elements
///    to be deleted, leaving the stream with a number of elements >= maxlen.
pub fn trim(self: *Stream, maxlen: usize, approx: bool) usize {
    if (self.length <= maxlen) {
        return 0;
    }

    var ri: raxlib.raxIterator = undefined;
    raxlib.raxStart(&ri, self.rax);
    defer raxlib.raxStop(&ri);
    _ = raxlib.raxSeek(&ri, "^", null, 0);

    var deleted: usize = 0;
    while (self.length > maxlen and raxlib.raxNext(&ri) != 0) {
        var lp: ?*ListPack = @ptrCast(@alignCast(ri.data.?));
        var p = lp.?.first();
        const entries: usize = @intCast(ListPack.getInteger(p.?));

        // Check if we can remove the whole node, and still have at
        // least maxlen elements.
        if (self.length - entries >= maxlen) {
            lp.?.free();
            _ = raxlib.raxRemove(self.rax, ri.key, ri.key_len, null);
            _ = raxlib.raxSeek(&ri, ">=", ri.key, ri.key_len);
            self.length -= entries;
            deleted += entries;
            continue;
        }

        // If we cannot remove a whole element, and approx is true,
        // stop here.
        if (approx) break;

        // Otherwise, we have to mark single entries inside the listpack
        // as deleted. We start by updating the entries/deleted counters.
        const to_delete = self.length - maxlen;
        assert(to_delete < entries);
        lp = lp.?.replaceInteger(&p, @intCast(entries - to_delete));
        p = lp.?.next(p.?); // Seek deleted field.
        const marked_deleted: u64 = @intCast(ListPack.getInteger(p.?));
        lp = lp.?.replaceInteger(&p, @intCast(marked_deleted + to_delete));
        p = lp.?.next(p.?); // Seek num-of-fields in the master entry.

        // Skip all the master fields.
        const master_fields_count = ListPack.getInteger(p.?);
        p = lp.?.next(p.?); // Seek the first field.
        for (0..@intCast(master_fields_count)) |_| {
            p = lp.?.next(p.?); // Skip all master fields.
        }
        p = lp.?.next(p.?); // Skip the zero master entry terminator.

        // 'p' is now pointing to the first entry inside the listpack.
        // We have to run entry after entry, marking entries as deleted
        // if they are already not deleted.
        while (p != null) {
            var flags = ListPack.getInteger(p.?);
            var to_skip: i64 = undefined;

            // Mark the entry as deleted.
            if (flags & ITEM_FLAG_DELETED == 0) {
                flags |= ITEM_FLAG_DELETED;
                lp = lp.?.replaceInteger(&p, flags);
                deleted += 1;
                self.length -= 1;
                if (self.length <= maxlen) break; // Enough entries deleted.
            }
            p = lp.?.next(p.?); // Skip ID ms delta.
            p = lp.?.next(p.?); // Skip ID seq delta.
            p = lp.?.next(p.?); // Seek num-fields or values (if compressed).
            if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                to_skip = master_fields_count;
            } else {
                to_skip = ListPack.getInteger(p.?);
                to_skip = to_skip * 2 + 1;
            }
            while (to_skip > 0) : (to_skip -= 1) {
                p = lp.?.next(p.?); // Skip the whole entry.
            }
            p = lp.?.next(p.?); // Skip the final lp-count field.
        }

        // Update the listpack with the new pointer.
        _ = raxlib.raxInsert(self.rax, ri.key, ri.key_len, lp, null);

        // If we are here, there was enough to delete in the current
        // node, so no need to go to the next node.
        break;
    }
    return deleted;
}

/// Create a new consumer group in the context of the stream, having the
/// specified name and last server ID. If a consumer group with the same name
/// already existed NULL is returned, otherwise the pointer to the consumer
/// group is returned.
pub fn createCG(self: *Stream, name: []u8, id: *Id) ?*CG {
    if (self.cgroups == null) {
        self.cgroups = raxlib.raxNew();
    }
    const cgroups = self.cgroups.?;
    if (raxlib.raxFind(cgroups, name.ptr, name.len) != raxlib.raxNotFound) {
        return null;
    }
    const cg = CG.create(id.*);
    _ = raxlib.raxInsert(cgroups, name.ptr, name.len, cg, null);
    return cg;
}

/// Lookup the consumer group in the specified stream and returns its
/// pointer, otherwise if there is no such group, NULL is returned.
pub fn lookupCG(self: *Stream, groupname: sds.String) ?*CG {
    const groups = self.cgroups orelse {
        return null;
    };
    const value = raxlib.raxFind(groups, groupname, sds.getLen(groupname));
    if (value == raxlib.raxNotFound) {
        return null;
    }
    return @ptrCast(@alignCast(value.?));
}

pub inline fn cast(ptr: *anyopaque) *Stream {
    return @ptrCast(@alignCast(ptr));
}

/// Destroy a stream, including the listpacks stored inside the radix tree.
pub fn destroy(self: *Stream) void {
    raxlib.raxFreeWithCallback(self.rax, freeListPackCallback);
    if (self.cgroups) |cgroups| {
        raxlib.raxFreeWithCallback(cgroups, freeCGCallback);
    }
    allocator.destroy(self);
}

fn freeListPackCallback(ptr: ?*anyopaque) callconv(.c) void {
    const lp: *ListPack = @ptrCast(@alignCast(ptr.?));
    lp.free();
}

fn freeCGCallback(ptr: ?*anyopaque) callconv(.c) void {
    const cg: *CG = @ptrCast(@alignCast(ptr.?));
    cg.destroy();
}

const std = @import("std");
const allocator = @import("allocator.zig");
const raxlib = @import("rax/rax.zig").rax;
const ListPack = @import("ListPack.zig");
const sds = @import("sds.zig");
const assert = std.debug.assert;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
const memlib = @import("mem.zig");
const memcmp = memlib.memcmp;
const memcpy = memlib.memcpy;
const sliceAsBytes = std.mem.sliceAsBytes;
const Object = @import("Object.zig");
const Server = @import("Server.zig");
const server = &Server.instance;
