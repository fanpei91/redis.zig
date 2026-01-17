// Static server configuration
pub const CONFIG_DEFAULT_DYNAMIC_HZ = true; // Adapt hz to # of clients.
pub const CONFIG_DEFAULT_HZ = 10; // Time interrupt calls/sec.
pub const CONFIG_MIN_HZ = 1;
pub const CONFIG_MAX_HZ = 500;
pub const CONFIG_DEFAULT_ACTIVE_REHASHING = true;
pub const CONFIG_DEFAULT_TCP_BACKLOG = 511; // TCP listen backlog.
pub const CONFIG_DEFAULT_CLIENT_TIMEOUT = 0; // Default client timeout: infinite
pub const CONFIG_DEFAULT_SERVER_PORT = 6379; // TCP port.
pub const CONFIG_DEFAULT_MAX_CLIENTS = 10000;
pub const CONFIG_DEFAULT_DBNUM = 16;
pub const CONFIG_DEFAULT_MAXMEMORY = 0;
pub const CONFIG_DEFAULT_MAXMEMORY_SAMPLES = 5;
pub const CONFIG_DEFAULT_LFU_LOG_FACTOR = 10;
pub const CONFIG_DEFAULT_LFU_DECAY_TIME = 1;
pub const CONFIG_MIN_RESERVED_FDS = 32;
pub const CONFIG_DEFAULT_UNIX_SOCKET_PERM = 0;
pub const CONFIG_BINDADDR_MAX = 16;
pub const CONFIG_MAX_LINE = 1024;
pub const CONFIG_DEFAULT_PROTECTED_MODE = true;
pub const CONFIG_DEFAULT_TCP_KEEPALIVE = 300;
pub const CONFIG_DEFAULT_PROTO_MAX_BULK_LEN = (512 * 1024 * 1024); // Bulk request max size
pub const CONFIG_AUTHPASS_MAX_LEN = 512;
pub const CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE = false;
pub const MAX_CLIENTS_PER_CLOCK_TICK = 200; // HZ is adapted based on that.
pub const OBJ_SHARED_INTEGERS = 10000;
pub const OBJ_SHARED_BULKHDR_LEN = 32;
pub const NET_MAX_WRITES_PER_EVENT = (1024 * 64);
pub const CONFIG_DEFAULT_RDB_FILENAME = "dump.rdb";
pub const CONFIG_DEFAULT_AOF_FILENAME = "appendonly.aof";
pub const CONFIG_DEFAULT_RDB_SAVE_INCREMENTAL_FSYNC = true;
pub const CONFIG_DEFAULT_RDB_CHECKSUM = true;
pub const CONFIG_DEFAULT_RDB_COMPRESSION = true;
pub const AOF_READ_DIFF_INTERVAL_BYTES = (1024 * 10);
pub const CONFIG_BGSAVE_RETRY_DELAY = 5; // Wait a few secs before trying again.
pub const CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE = false;
pub const CONFIG_DEFAULT_AOF_LOAD_TRUNCATED = true;
pub const CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC = true;
pub const CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE = true;
pub const AOF_REWRITE_ITEMS_PER_CMD = 64;
pub const AOF_REWRITE_PERC = 100;
pub const AOF_REWRITE_MIN_SIZE = (64 * 1024 * 1024);

pub const CRON_DBS_PER_CALL = 16;
pub const ACTIVE_EXPIRE_CYCLE_SLOW = 0;
pub const ACTIVE_EXPIRE_CYCLE_FAST = 1;
pub const ACTIVE_EXPIRE_CYCLE_FAST_DURATION = 1000; // Microseconds
pub const ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC = 25; // CPU max % for keys collection
pub const ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP = 20; // Loopkups per loop.

// Protocol and I/O related defines
pub const PROTO_MAX_QUERYBUF_LEN = 1024 * 1024 * 1024; // 1GB max query buffer.
pub const PROTO_IOBUF_LEN = 1024 * 16; // Generic I/O buffer size
pub const PROTO_MBULK_BIG_ARG = (1024 * 32);
pub const PROTO_INLINE_MAX_SIZE = (1024 * 64); // Max size of inline reads
pub const PROTO_REPLY_CHUNK_BYTES = 16 * 1024; // 16k output buffer
pub const REDIS_AUTOSYNC_BYTES = (1024 * 1024 * 32); // fdatasync every 32MB

// Client request types
pub const PROTO_REQ_INLINE = 1;
pub const PROTO_REQ_MULTIBULK = 2;

// --- SHUTDOWN flags ---
/// No flags.
pub const SHUTDOWN_NOFLAGS = 0;
/// Force SAVE on SHUTDOWN even if no save
/// points are configured.
pub const SHUTDOWN_SAVE = 1;
/// Don't SAVE on SHUTDOWN.
pub const SHUTDOWN_NOSAVE = 2;

// Command flags. Please check the command table defined in the redis.c file
// for more information about the meaning of every flag.
pub const CMD_WRITE = (1 << 0); // "w" flag
pub const CMD_READONLY = (1 << 1); // "r" flag
pub const CMD_DENYOOM = (1 << 2); // "m" flag
pub const CMD_ADMIN = (1 << 4); // "a" flag
pub const CMD_PUBSUB = (1 << 5); // "p" flag
pub const CMD_NOSCRIPT = (1 << 6); // "s" flag
pub const CMD_RANDOM = (1 << 7); // "R" flag
pub const CMD_SORT_FOR_SCRIPT = (1 << 8); // "S" flag
pub const CMD_LOADING = (1 << 9); // "l" flag
pub const CMD_STALE = (1 << 10); // "t" flag
pub const CMD_SKIP_MONITOR = (1 << 11); // "M" flag
pub const CMD_ASKING = (1 << 12); // "k" flag
pub const CMD_FAST = (1 << 13); // "F" flag

// Command call flags, see call() function
pub const CMD_CALL_NONE = 0;
pub const CMD_CALL_SLOWLOG = (1 << 0);
pub const CMD_CALL_STATS = (1 << 1);
pub const CMD_CALL_PROPAGATE_AOF = (1 << 2);
pub const CMD_CALL_PROPAGATE_REPL = (1 << 3);
pub const CMD_CALL_PROPAGATE = (CMD_CALL_PROPAGATE_AOF | CMD_CALL_PROPAGATE_REPL);
pub const CMD_CALL_FULL = (CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_PROPAGATE);

// Command propagation flags, see propagate() function
pub const PROPAGATE_NONE = 0;
pub const PROPAGATE_AOF = 1;
pub const PROPAGATE_REPL = 2;

// AOF states
pub const AOF_OFF = 0; // AOF is off
pub const AOF_ON = 1; // AOF is on
pub const AOF_WAIT_REWRITE = 2; // AOF waits rewrite to start appending

// Client flags
pub const CLIENT_MULTI = (1 << 3); // This client is in a MULTI context
pub const CLIENT_BLOCKED = (1 << 4); // The client is waiting in a blocking operation
pub const CLIENT_DIRTY_CAS = (1 << 5); // Watched keys modified. EXEC will fail.
pub const CLIENT_CLOSE_AFTER_REPLY = (1 << 6); // Close after writing entire reply.
pub const CLIENT_UNBLOCKED = (1 << 7); // This client was unblocked and is stored in server.unblocked_clients
pub const CLIENT_LUA = (1 << 8); // This is a non connected client used by Lua
pub const CLIENT_CLOSE_ASAP = (1 << 10); // Close this client ASAP
pub const CLIENT_UNIX_SOCKET = (1 << 11); // Client connected via Unix domain socket
pub const CLIENT_DIRTY_EXEC = (1 << 12); // EXEC will fail for errors while queueing
pub const CLIENT_FORCE_AOF = (1 << 14); // Force AOF propagation of current cmd.
pub const CLIENT_PUBSUB = (1 << 18); // Client is in Pub/Sub mode.
pub const CLIENT_PREVENT_AOF_PROP = (1 << 19); // Don't propagate to AOF.
pub const CLIENT_PREVENT_PROP = (CLIENT_PREVENT_AOF_PROP);
pub const CLIENT_PENDING_WRITE = (1 << 21); // Client has output to send but a write handler is yet not installed.
pub const CLIENT_LUA_DEBUG_SYNC = (1 << 26); // EVAL debugging without fork()
pub const CLIENT_PROTECTED = (1 << 28); // Client should not be freed for now.

// Client block type (btype field in Client structure)
// if CLIENT_BLOCKED flag is set.
pub const BLOCKED_NONE = 0; // Not blocked, no CLIENT_BLOCKED flag set.
pub const BLOCKED_LIST = 1; // BLPOP & co.
pub const BLOCKED_MODULE = 3;
pub const BLOCKED_STREAM = 4; // XREAD.
pub const BLOCKED_ZSET = 5; // BZPOP et al.

// Units
pub const UNIT_SECONDS = 0;
pub const UNIT_MILLISECONDS = 1;

// RDB active child save type.
pub const RDB_CHILD_TYPE_NONE = 0;
pub const RDB_CHILD_TYPE_DISK = 1; // RDB is written to disk.
pub const RDB_CHILD_TYPE_SOCKET = 2; // RDB is written to slave socket.

pub const LRU_BITS = 24;
pub const LRU_CLOCK_MAX = ((1 << LRU_BITS) - 1); // Max value of obj->lru
pub const LRU_CLOCK_RESOLUTION = 1000; // LRU clock resolution in ms

// Redis maxmemory strategies. Instead of using just incremental number
// for this defines, we use a set of flags so that testing for certain
// properties common to multiple policies is faster.
pub const MAXMEMORY_FLAG_LRU = (1 << 0);
pub const MAXMEMORY_FLAG_LFU = (1 << 1);
pub const MAXMEMORY_FLAG_ALLKEYS = (1 << 2);
pub const MAXMEMORY_FLAG_NO_SHARED_INTEGERS = MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU;

pub const MAXMEMORY_VOLATILE_LRU = ((0 << 8) | MAXMEMORY_FLAG_LRU);
pub const MAXMEMORY_VOLATILE_LFU = ((1 << 8) | MAXMEMORY_FLAG_LFU);
pub const MAXMEMORY_VOLATILE_TTL = (2 << 8);
pub const MAXMEMORY_VOLATILE_RANDOM = (3 << 8);
pub const MAXMEMORY_ALLKEYS_LRU = ((4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS);
pub const MAXMEMORY_ALLKEYS_LFU = ((5 << 8) | MAXMEMORY_FLAG_LFU | MAXMEMORY_FLAG_ALLKEYS);
pub const MAXMEMORY_ALLKEYS_RANDOM = ((6 << 8) | MAXMEMORY_FLAG_ALLKEYS);
pub const MAXMEMORY_NO_EVICTION = 7 << 8;
pub const CONFIG_DEFAULT_MAXMEMORY_POLICY = MAXMEMORY_NO_EVICTION;

pub const LOOKUP_NONE = 0;
pub const LOOKUP_NOTOUCH = (1 << 0);

// When configuring the server eventloop, we setup it so that the total number
// of file descriptors we can handle are server.maxclients + RESERVED_FDS +
// a few more to stay safe. Since RESERVED_FDS defaults to 32, we add 96
// in order to make sure of not over provisioning more than 128 fds.
pub const CONFIG_FDSET_INCR = CONFIG_MIN_RESERVED_FDS + 96;

// List defaults
pub const OBJ_LIST_MAX_ZIPLIST_SIZE = -2;
pub const OBJ_LIST_COMPRESS_DEPTH = 0;

// Hash table parameters
pub const HASHTABLE_MIN_FILL = 10; // Minimal hash table fill 10%

// Append only defines
pub const AOF_FSYNC_NO = 0;
pub const AOF_FSYNC_ALWAYS = 1;
pub const AOF_FSYNC_EVERYSEC = 2;
pub const CONFIG_DEFAULT_AOF_FSYNC = AOF_FSYNC_EVERYSEC;

// Zipped structures related defaults
pub const OBJ_HASH_MAX_ZIPLIST_ENTRIES = 512;
pub const OBJ_HASH_MAX_ZIPLIST_VALUE = 64;
pub const OBJ_SET_MAX_INTSET_ENTRIES = 512;
pub const OBJ_ZSET_MAX_ZIPLIST_ENTRIES = 128;
pub const OBJ_ZSET_MAX_ZIPLIST_VALUE = 64;
pub const OBJ_STREAM_NODE_MAX_BYTES = 4096;
pub const OBJ_STREAM_NODE_MAX_ENTRIES = 100;

// Hash data type
pub const HASH_SET_TAKE_FIELD = (1 << 0);
pub const HASH_SET_TAKE_VALUE = (1 << 1);
pub const HASH_SET_COPY = 0;

pub const OBJ_HASH_KEY = 1;
pub const OBJ_HASH_VALUE = 2;

pub const CHILD_INFO_MAGIC = 0xC17DDA7A12345678;
pub const CHILD_INFO_TYPE_RDB = 0;
pub const CHILD_INFO_TYPE_AOF = 1;

// Scripting
pub const LUA_SCRIPT_TIME_LIMIT = 5000; // milliseconds

const Server = @This();
// General
configfile: ?sds.String, // Absolute config file path.
el: *ae.EventLoop,
lruclock: atomic.Value(u32), // Clock for LRU eviction
shutdown_asap: bool, // SHUTDOWN needed ASAP
dynamic_hz: bool, // Change hz value depending on # of clients.
config_hz: u32, // Configured HZ value. May be different than the actual 'hz' field value if dynamic-hz is enabled.
hz: u32, // serverCron() calls frequency in hertz
arch_bits: i32, // 32 or 64 depending on @bitSizeOf(*anyopaque)
cronloops: i32, // Number of times the cron function run
requirepass: ?[]u8, // Pass for AUTH command, or null
db: []Database,
commands: *Commands.HashMap, // Command table
activerehashing: bool, // Incremental rehash in serverCron()
// Networking
clients: *ClientList, // List of active clients
clients_index: [*c]raxlib.rax, // Active clients dictionary by client ID.
clients_to_close: *ClientList, // Clients to close asynchronously
clients_pending_write: *ClientList, // There is to write or install handler.
next_client_id: atomic.Value(usize), // Next client unique ID. Incremental.
port: u16,
tcp_backlog: u32, // TCP listen() backlog
bindaddr: [CONFIG_BINDADDR_MAX][]const u8, // Addresses we should bind to
bindaddr_count: usize, // Number of addresses in []server.bindaddr
ipfd: [CONFIG_BINDADDR_MAX]i32, // TCP socket file descriptors
ipfd_count: usize, // Used slots in []ipfd
unixsocket: ?[]u8, // UNIX socket path
sofd: i32, // Unix socket file descriptor
unixsocketperm: std.posix.mode_t, // UNIX socket permission
protected_mode: bool, // Don't accept external connections.
fixed_time_expire: i64, // If > 0, expire keys against server.mstime.
// RDB / AOF loading information
loading: bool, //  We are loading data from disk if true
loading_start_time: i64,
loading_total_bytes: u64,
loading_loaded_bytes: u64,
loading_process_events_interval_bytes: u64,
// Fast pointers to often looked up command
expireCommand: *const Command,
pexpireCommand: *const Command,
zpopminCommand: *const Command,
zpopmaxCommand: *const Command,
multiCommand: *const Command,
lpopCommand: *const Command,
rpopCommand: *const Command,
lpushCommand: *const Command,
xgroupCommand: *const Command,
xclaimCommand: *const Command,
delCommand: *const Command,
sremCommand: *const Command,
// Configuration
active_expire_enabled: bool,
maxidletime: u32, // Client timeout in seconds
dbnum: u32, // Total number of configured DBs
tcpkeepalive: i32, // Set SO_KEEPALIVE if non-zero.
client_max_querybuf_len: usize, // Limit for client query buffer length
// RDB persistence
dirty: i64, // Changes to DB from the last save
dirty_before_bgsave: i64, // Used to restore dirty on failed BGSAVE
rdb_child_pid: posix.pid_t, // PID of RDB saving child
rdb_child_type: i32, // Type of save by active child.
rdb_filename: []u8, // Name of RDB file
rdb_save_incremental_fsync: bool, // fsync incrementally while rdb saving?
lastsave: i64, // Unix time of last successful save
lastbgsave_try: i64, // Unix time of last attempted bgsave
rdb_checksum: bool, // Use RDB checksum?
rdb_compression: bool, // Use compression in RDB?
rdb_bgsave_scheduled: bool, // BGSAVE when possible if true.
lastbgsave_status: bool, // OK: true, ERR: false
rdb_save_time_start: i64, // Current RDB save start time.
saveparams: std.ArrayList(SaveParam), // Save points array for RDB
// AOF persistence
aof_child_pid: posix.pid_t, //  PID if rewriting process
aof_state: i32, // AOF_(ON|OFF|WAIT_REWRITE)
aof_fd: posix.fd_t, // File descriptor of currently selected AOF file
aof_filename: []u8, // Name of the AOF file
aof_selected_db: i32, // Currently selected DB in AOF
aof_buf: sds.String, // AOF buffer, written before entering the event loop
aof_flush_postponed_start: i64, // UNIX time of postponed AOF flush
aof_last_write_status: bool, // OK: true, ERR: false
aof_fsync: i32, // Kind of fsync() policy
aof_no_fsync_on_rewrite: bool, // Don't fsync if a rewrite is in progress.
aof_current_size: usize, // AOF current size.
aof_fsync_offset: usize, // AOF offset which is already synced to disk.
aof_last_fsync: i64, // UNIX time of last fsync()
aof_delayed_fsync: u64, // delayed AOF fsync() counter
aof_last_write_err: ?anyerror, // Valid if aof_last_write_status is false
aof_load_truncated: bool, // Don't stop on unexpected AOF EOF.
aof_rewrite_base_size: usize, // AOF size on latest startup or rewrite.
aof_rewrite_scheduled: bool, // Rewrite once BGSAVE terminates.
aof_rewrite_time_start: i64, // Current AOF rewrite start time.
aof_rewrite_time_last: i64, // Time used by last AOF rewrite run.
// AOF pipes used to communicate between parent and child during rewrite.
aof_pipe_write_data_to_child: posix.fd_t,
aof_pipe_read_data_from_parent: posix.fd_t,
aof_pipe_write_ack_to_parent: posix.fd_t,
aof_pipe_read_ack_from_child: posix.fd_t,
aof_pipe_write_ack_to_child: posix.fd_t,
aof_pipe_read_ack_from_parent: posix.fd_t,
aof_stop_sending_diff: bool, // If true stop sending accumulated diffs to child process.
aof_child_diff: sds.String, // AOF diff accumulator child side.
aof_rewrite_incremental_fsync: bool, // fsync incrementally while aof rewriting?
aof_use_rdb_preamble: bool, // Use RDB preamble on AOF rewrites.
aof_rewrite_buf_blocks: ?*List(void, *aof.RwBlock), // Hold changes during an AOF rewrite.
aof_lastbgrewrite_status: bool, // OK: true, ERR: false
aof_rewrite_perc: i32, // Rewrite AOF if % growth is > M and...
aof_rewrite_min_size: usize, // the AOF file is at least N bytes.
// Propagation of commands in AOF / replication
also_prpagate: RedisOpArray, // Additional command to propagate.
// Limits
maxclients: u32, // Max number of simultaneous clients
maxmemory: u64, // Max number of memory bytes to use
maxmemory_policy: i32, // Policy for key eviction
maxmemory_samples: i32, // Pricision of random sampling
proto_max_bulk_len: usize, // Protocol bulk length maximum size.
lfu_log_factor: i32, // LFU logarithmic counter factor.
lfu_decay_time: i32, // LFU counter decay factor.
// time cache
unixtime: atomic.Value(i64), // Unix time sampled every cron cycle.
mstime: i64, // 'unixtime' in milliseconds.
ustime: i64, // 'unixtime' in microseconds.
// ---PubSub---
/// Map channels to list of subscribed clients
pubsub_channels: *dict.Dict(*Object, *ClientList),
/// A list of pubsub_patterns
pubsub_patterns: *List(*pubsub.Pattern, *pubsub.Pattern),
// Lazy free
lazyfree_lazy_expire: bool,
// List parameters
list_max_ziplist_size: i32,
list_compress_depth: i32,
// Zip structure config, see redis.conf for more information
hash_max_ziplist_value: usize,
hash_max_ziplist_entries: usize,
set_max_intset_entries: usize,
zset_max_ziplist_entries: usize,
zset_max_ziplist_value: usize,
stream_node_max_bytes: usize,
stream_node_max_entries: i64,
// Blocked clients
ready_keys: *ReadyKeys, // List of ReadyList structures for BLPOP & co
unblocked_clients: *ClientList, // list of clients to unblock before next loop
/// The Lua interpreter. We use just one for all clients
lua: scripting.Lua,

pub fn create(configfile: ?sds.String, options: ?sds.String) !void {
    server.configfile = configfile;
    server.initConfig();
    try config.load(server, configfile, options);
    try server.init();
    try bio.init();
}

fn initConfig(self: *Server) void {
    self.config_hz = CONFIG_DEFAULT_HZ;
    self.arch_bits = @bitSizeOf(*anyopaque);
    self.activerehashing = CONFIG_DEFAULT_ACTIVE_REHASHING;
    self.requirepass = null;
    self.commands = Commands.HashMap.create(Commands.vtable);
    self.populateCommandTable();
    self.expireCommand = self.lookupCommandByString("expire");
    self.pexpireCommand = self.lookupCommandByString("pexpire");
    self.zpopminCommand = self.lookupCommandByString("zpopmin");
    self.zpopmaxCommand = self.lookupCommandByString("zpopmax");
    self.multiCommand = self.lookupCommandByString("multi");
    self.lpopCommand = self.lookupCommandByString("lpop");
    self.rpopCommand = self.lookupCommandByString("rpop");
    self.lpushCommand = self.lookupCommandByString("lpush");
    self.xgroupCommand = self.lookupCommandByString("xgroup");
    self.xclaimCommand = self.lookupCommandByString("xclaim");
    self.delCommand = self.lookupCommandByString("del");
    self.sremCommand = self.lookupCommandByString("srem");
    self.shutdown_asap = false;
    self.dynamic_hz = CONFIG_DEFAULT_DYNAMIC_HZ;
    self.port = CONFIG_DEFAULT_SERVER_PORT;
    self.tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
    self.bindaddr_count = 0;
    self.ipfd_count = 0;
    self.unixsocket = null;
    self.sofd = -1;
    self.unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
    self.protected_mode = CONFIG_DEFAULT_PROTECTED_MODE;
    self.loading = false;
    self.loading_process_events_interval_bytes = 1024 * 1024 * 2;
    self.active_expire_enabled = true;
    self.maxidletime = CONFIG_DEFAULT_CLIENT_TIMEOUT;
    self.dbnum = CONFIG_DEFAULT_DBNUM;
    self.tcpkeepalive = CONFIG_DEFAULT_TCP_KEEPALIVE;
    self.client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;

    self.rdb_filename = allocator.dupe(u8, CONFIG_DEFAULT_RDB_FILENAME);
    self.rdb_save_incremental_fsync = CONFIG_DEFAULT_RDB_SAVE_INCREMENTAL_FSYNC;
    self.saveparams = .empty;
    config.appendServerSaveParams(self, 60 * 60, 1); // save after 1 hour and 1 change
    config.appendServerSaveParams(self, 300, 100); // save after 5 minutes and 100 changes
    config.appendServerSaveParams(self, 60, 10000); // save after 1 minute and 10000 changes
    self.rdb_checksum = CONFIG_DEFAULT_RDB_CHECKSUM;
    self.rdb_compression = CONFIG_DEFAULT_RDB_COMPRESSION;

    self.aof_state = AOF_OFF;
    self.aof_filename = allocator.dupe(u8, CONFIG_DEFAULT_AOF_FILENAME);
    self.aof_selected_db = -1; // Make sure the first time will not match
    self.aof_flush_postponed_start = 0;
    self.aof_fd = -1;
    self.aof_fsync = CONFIG_DEFAULT_AOF_FSYNC;
    self.aof_no_fsync_on_rewrite = CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE;
    self.aof_last_fsync = std.time.timestamp();
    self.aof_delayed_fsync = 0;
    self.aof_load_truncated = CONFIG_DEFAULT_AOF_LOAD_TRUNCATED;
    self.aof_rewrite_base_size = 0;
    self.aof_rewrite_scheduled = false;
    self.aof_rewrite_time_start = -1;
    self.aof_rewrite_time_last = -1;
    self.aof_rewrite_incremental_fsync = CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC;
    self.aof_use_rdb_preamble = CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE;
    self.aof_rewrite_buf_blocks = null;
    self.aof_lastbgrewrite_status = true;
    self.aof_rewrite_perc = AOF_REWRITE_PERC;
    self.aof_rewrite_min_size = AOF_REWRITE_MIN_SIZE;

    self.maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    self.maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    self.maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
    self.maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;
    self.proto_max_bulk_len = CONFIG_DEFAULT_PROTO_MAX_BULK_LEN;
    self.lfu_log_factor = CONFIG_DEFAULT_LFU_LOG_FACTOR;
    self.lfu_decay_time = CONFIG_DEFAULT_LFU_DECAY_TIME;
    self.lruclock = .init(0);
    self.lruclock.store(evict.getLRUClock());

    self.unixtime = .init(0);
    self.updateCachedTime();

    self.lazyfree_lazy_expire = CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE;
    self.list_max_ziplist_size = OBJ_LIST_MAX_ZIPLIST_SIZE;
    self.list_compress_depth = OBJ_LIST_COMPRESS_DEPTH;
    self.hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;
    self.hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
    self.set_max_intset_entries = OBJ_SET_MAX_INTSET_ENTRIES;
    self.zset_max_ziplist_entries = OBJ_ZSET_MAX_ZIPLIST_ENTRIES;
    self.zset_max_ziplist_value = OBJ_ZSET_MAX_ZIPLIST_VALUE;
    self.stream_node_max_bytes = OBJ_STREAM_NODE_MAX_BYTES;
    self.stream_node_max_entries = OBJ_STREAM_NODE_MAX_ENTRIES;
}

fn init(self: *Server) !void {
    setupSignalHandlers();

    self.hz = self.config_hz;
    self.cronloops = 0;

    self.dirty = 0;
    self.rdb_child_pid = -1;
    self.rdb_child_type = RDB_CHILD_TYPE_NONE;
    self.lastsave = std.time.timestamp(); // At startup we consider the DB saved.
    self.lastbgsave_try = 0; // At startup we never tried to BGSAVE.
    self.lastbgsave_status = true;
    self.rdb_save_time_start = -1;
    self.rdb_bgsave_scheduled = false;

    self.aof_child_pid = -1;
    self.aof_last_write_status = true;
    self.aof_buf = sds.empty(allocator.impl);
    server.aof_last_write_err = null;
    aof.rewriteBufferReset();

    self.pubsub_channels = .create(&.{
        .hash = Object.hash,
        .eql = Object.equalStrings,
        .dupeKey = Object.incrRefCount,
        .freeKey = Object.decrRefCount,
        .freeVal = ClientList.release,
    });
    self.pubsub_patterns = .create(&.{
        .eql = pubsub.Pattern.eql,
        .freeVal = pubsub.Pattern.destroy,
    });

    self.ready_keys = ReadyKeys.create(&.{
        .freeVal = blocked.ReadyList.destroy,
    });
    self.unblocked_clients = ClientList.create(&.{});
    self.fixed_time_expire = 0;
    self.clients = ClientList.create(&.{});
    self.clients_index = raxlib.raxNew();
    self.clients_pending_write = ClientList.create(&.{});
    self.clients_to_close = ClientList.create(&.{});
    self.next_client_id = .init(1);

    shared = Object.Shared.create();
    try self.adjustOpenFilesLimit();
    self.el = try ae.EventLoop.create(
        @intCast(self.maxclients + CONFIG_FDSET_INCR),
    );

    self.db = allocator.alloc(Database, self.dbnum);
    for (0..self.dbnum) |i| {
        self.db[i] = Database.create(i);
    }

    // Open the TCP listening socket for the user commands.
    if (self.port != 0) try self.listenToPort();

    // Open the listening Unix domain socket.
    if (self.unixsocket) |unixsocket| {
        posix.unlink(unixsocket) catch {};
        self.sofd = anet.unixServer(
            unixsocket,
            self.unixsocketperm,
            self.tcp_backlog,
        ) catch |err| {
            logging.warn("Openning Unix socket: {}", .{err});
            return err;
        };
        try anet.nonBlock(self.sofd);
    }

    if (self.ipfd_count == 0 and self.sofd < 0) {
        logging.warn("Configured to not listen anywhere, exiting", .{});
        return error.NoListenSockets;
    }

    // Create the timer callback, this is our way to process many background
    // operations incrementally, like clients timeout, eviction of unaccessed
    // expired keys and so forth.
    _ = self.el.createTimeEvent(
        1,
        serverCron,
        null,
        null,
    );
    // Create an event handler for accepting new connections in TCP and Unix
    // domain sockets.
    for (self.ipfd[0..self.ipfd_count]) |fd| {
        self.el.createFileEvent(
            fd,
            ae.READABLE,
            networking.acceptHandler,
            null,
        ) catch |err| {
            logging.warn("Unrecoverable error creating server.ipfd file event.", .{});
            return err;
        };
    }
    if (self.sofd > 0) {
        self.el.createFileEvent(
            self.sofd,
            ae.READABLE,
            networking.acceptHandler,
            null,
        ) catch |err| {
            logging.warn("Unrecoverable error creating server.sofd file event.", .{});
            return err;
        };
    }

    // Open the AOF file if needed.
    if (self.aof_state == AOF_ON) {
        const flags: posix.O = .{
            .ACCMODE = .WRONLY,
            .APPEND = true,
            .CREAT = true,
        };
        self.aof_fd = posix.open(
            server.aof_filename,
            flags,
            0o644,
        ) catch |err| {
            logging.warn("Can't open the append-only file: {}", .{err});
            std.process.exit(1);
        };
    }

    // 32 bit instances are limited to 4GB of address space, so if there is
    // no explicit limit in the user provided configuration we set a limit
    // at 3 GB using maxmemory with 'noeviction' policy'. This avoids
    // useless crashes of the Redis instance for out of memory.
    if (self.arch_bits == 32 and self.maxmemory == 0) {
        logging.warn(
            "32 bit instance detected but no memory limit set. " ++
                "Setting 3 GB maxmemory limit with 'noeviction' policy now.",
            .{},
        );
        self.maxmemory = 3 * 1024 * 1024 * 1024; // 3GB
        self.maxmemory_policy = MAXMEMORY_NO_EVICTION;
    }

    self.lua.init(true);
    self.lua.time_limit = LUA_SCRIPT_TIME_LIMIT;
}

/// This function will try to raise the max number of open files accordingly to
/// the configured max number of clients. It also reserves a number of file
/// descriptors (CONFIG_MIN_RESERVED_FDS) for extra operations of
/// persistence, listening sockets, log files and so forth.
///
/// If it will not be possible to set the limit accordingly to the configured
/// max number of clients, the function will do the reverse setting
/// server.maxclients to the value that we can actually handle.
fn adjustOpenFilesLimit(self: *Server) !void {
    const maxfiles: posix.rlim_t = self.maxclients + CONFIG_MIN_RESERVED_FDS;
    if (posix.getrlimit(posix.rlimit_resource.NOFILE)) |limit| {
        const oldlimit: posix.rlim_t = limit.cur;
        // Set the max number of files if the current limit is not enough
        // for our needs.
        if (oldlimit < maxfiles) {
            var setrlimit_error: anyerror = undefined;
            var bestlimit = maxfiles;
            while (bestlimit > oldlimit) {
                const decr_step: posix.rlim_t = 16;
                var rlimit = limit;
                rlimit.cur = bestlimit;
                rlimit.max = bestlimit;
                posix.setrlimit(posix.rlimit_resource.NOFILE, rlimit) catch |err| {
                    setrlimit_error = err;
                    if (bestlimit < decr_step) break;
                    bestlimit -= decr_step;
                    continue;
                };
                break;
            }
            if (bestlimit < oldlimit) bestlimit = oldlimit;
            if (bestlimit < maxfiles) {
                const old_maxclients = self.maxclients;
                self.maxclients = @intCast(bestlimit -| CONFIG_MIN_RESERVED_FDS);
                if (bestlimit <= CONFIG_MIN_RESERVED_FDS) {
                    logging.warn(
                        "Your current 'ulimit -n' " ++
                            "of {} is not enough for the server to start. " ++
                            "Please increase your open file limit to at least to {}.",
                        .{ oldlimit, maxfiles },
                    );
                    return error.MaxOpenFilesLimitNotEnough;
                }
                logging.warn(
                    "You requested maxclients of {} " ++
                        "requiring at least {} max file descriptors.",
                    .{ old_maxclients, maxfiles },
                );
                logging.warn(
                    "Server can't set maximum open files to {} because of OS error: {}.",
                    .{ maxfiles, setrlimit_error },
                );
                logging.warn("Current maximum open files is {}. " ++
                    "maxclients has been reduced to {} to compensate for " ++
                    "low ulimit. " ++
                    "If you need higher maxclients increase 'ulimit -n'.", .{
                    bestlimit,
                    self.maxclients,
                });
            } else {
                logging.notice(
                    "Increased maximum number of open files " ++
                        "to {} (it was originally set to {}).",
                    .{ maxfiles, oldlimit },
                );
            }
        }
    } else |err| {
        logging.warn(
            "Unable to obtain the current NOFILE limit ({}), " ++
                "assuming 1024 and setting the max clients configuration accordingly.",
            .{err},
        );
        self.maxclients = 1024 - CONFIG_MIN_RESERVED_FDS;
    }
}

/// We take a cached value of the unix time in the global state because with
/// virtual memory and aging there is to store the current time in objects at
/// every object access, and accuracy is not needed. To access a global var is
/// a lot faster than calling std.time.microTimestamp().
pub fn updateCachedTime(self: *Server) void {
    self.ustime = std.time.microTimestamp();
    self.mstime = @divFloor(self.ustime, std.time.us_per_ms);
    const unixtime = @divFloor(self.mstime, std.time.ms_per_s);
    self.unixtime.store(unixtime);
}

/// Initialize a set of file descriptors to listen to the specified 'port'
/// binding the addresses specified in the Redis server configuration.
fn listenToPort(self: *Server) !void {
    // Force binding of 0.0.0.0/:: if no bind address is specified.
    var bindaddr = self.bindaddr;
    var bindaddr_count = self.bindaddr_count;
    if (bindaddr_count == 0) {
        bindaddr[0] = "0.0.0.0";
        bindaddr[1] = "::";
        bindaddr_count = 2;
    }
    var i: usize = 0;
    for (bindaddr[0..bindaddr_count]) |addr| {
        const fd = anet.tcpServer(
            self.port,
            addr,
            self.tcp_backlog,
        ) catch |err| {
            logging.warn(
                "Could not create server TCP listening socket {s}:{}: {}",
                .{ addr, self.port, err },
            );
            continue;
        };
        self.ipfd[i] = fd;
        self.ipfd_count += 1;
        anet.nonBlock(fd) catch {};
        i += 1;
    }
    if (self.ipfd_count == 0) return error.NoListeningSockets;
}

fn setupSignalHandlers() void {
    // Zig can't compile c.SIG_IGN
    const SIG_IGN: *const fn (c_int) callconv(.c) void = @ptrFromInt(1);
    _ = libc.signal(libc.SIGHUP, SIG_IGN);
    _ = libc.signal(libc.SIGPIPE, SIG_IGN);

    var act: posix.Sigaction = .{
        .handler = .{
            .handler = sigShutdownHandler,
        },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &act, null);
    posix.sigaction(posix.SIG.TERM, &act, null);
}

fn sigShutdownHandler(sig: i32) callconv(.c) void {
    // SIGINT is often delivered via Ctrl+C in an interactive session.
    // If we receive the signal the second time, we interpret this as
    // the user really wanting to quit ASAP without waiting to persist
    // on disk.
    if (server.shutdown_asap and sig == posix.SIG.INT) {
        logging.warn("You insist... exiting now.", .{});
        rdb.rdbRemoveTempFile(libc.getpid());
        //Exit with an error since this was not a clean shutdown.
        std.process.exit(1);
    } else if (server.loading) {
        logging.warn(
            "Received shutdown signal during loading, exiting now.",
            .{},
        );
        std.process.exit(0);
    }
    switch (sig) {
        posix.SIG.INT => logging.warn("Received SIGINT scheduling shutdown...", .{}),
        posix.SIG.TERM => logging.warn("Received SIGTERM scheduling shutdown...", .{}),
        else => logging.warn("Received shutdown signal, scheduling shutdown...", .{}),
    }
    server.shutdown_asap = true;
}

pub fn up(self: *Server) !void {
    self.el.setBeforeSleepProc(beforeSleep);
    try self.el.main();
}

/// This function gets called every time Redis is entering the
/// main loop of the event driven library, that is, before to sleep
/// for ready file descriptors.
fn beforeSleep(el: *ae.EventLoop) !void {
    _ = el;

    // Run a fast expire cycle (the called function will return
    // ASAP if a fast cycle is not needed).
    if (server.active_expire_enabled) {
        expire.activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST);
    }

    // Try to process pending commands for clients that were just unblocked.
    if (server.unblocked_clients.len != 0) {
        blocked.processUnblockedClients();
    }

    // Write the AOF buffer on disk.
    aof.flushAppendOnlyFile(false);

    // Handle write with pending output buffers.
    _ = networking.handleClientsWithPendingWrites() catch {};
}

/// This is our timer interrupt, called server.hz times per second.
fn serverCron(
    el: *ae.EventLoop,
    id: i64,
    clicent_data: ae.ClientData,
) !i32 {
    _ = el;
    _ = id;
    _ = clicent_data;

    // Update the time cache.
    server.updateCachedTime();

    // Adapt the server.hz value to the number of configured clients. If we have
    // many clients, we want to call serverCron() with an higher frequency.
    if (server.dynamic_hz) {
        while (server.clients.len / server.hz > MAX_CLIENTS_PER_CLOCK_TICK) {
            server.hz *= 2;
            if (server.hz > CONFIG_MAX_HZ) {
                server.hz = CONFIG_MAX_HZ;
                break;
            }
        }
    }

    // We have just LRU_BITS bits per object for LRU information.
    // So we use an (eventually wrapping) LRU clock.
    //
    // Note that even if the counter wraps it's not a big problem,
    // everything will still work but some object will appear younger
    // to Redis. However for this to happen a given object should never be
    // touched for all the time needed to the counter to wrap, which is
    // not likely.
    //
    // Note that you can change the resolution altering the
    // LRU_CLOCK_RESOLUTION define.
    server.lruclock.store(evict.getLRUClock());

    // We received a SIGTERM, shutting down here in a safe way, as it is
    // not ok doing so inside the signal handler.
    if (server.shutdown_asap) {
        if (prepareForShutdown(SHUTDOWN_NOFLAGS)) {
            std.process.exit(0);
        }
        logging.warn(
            "SIGTERM received but errors trying to shut down the server, " ++
                "check the logs for more information",
            .{},
        );
        server.shutdown_asap = false;
    }

    // Show some info about non-empty databases
    if (runWithPeriod(5000)) {
        for (server.db, 0..) |*db, i| {
            const size = db.dict.slots();
            const used = db.dict.size();
            const vkeys = db.expires.size();
            if (used > 0 or vkeys > 0) {
                logging.verbose(
                    "DB {}: {} keys ({} volatile) in {} slots HT.",
                    .{ i, used, vkeys, size },
                );
            }
        }
    }

    // We need to do a few operations on clients asynchronously.
    clientsCron();

    // Handle background operations on Redis databases.
    databaseCron();

    // Start a scheduled AOF rewrite if this was requested by the user while
    // a BGSAVE was in progress.
    if (server.rdb_child_pid == -1 and server.aof_child_pid == -1 and
        server.aof_rewrite_scheduled)
    {
        _ = aof.rewriteAppendOnlyFileBackground();
    }

    // Check if a background saving of AOF rewrite in progress terminated.
    if (server.rdb_child_pid != -1 or server.aof_child_pid != -1) {
        var statloc: c_int = undefined;
        const pid = libc.wait3(&statloc, libc.WNOHANG, null);
        if (pid != 0) {
            const exitcode = libc.WEXITSTATUS(statloc);
            var bysignal: c_int = 0;
            if (libc.WIFSIGNALED(statloc)) {
                bysignal = libc.WTERMSIG(statloc);
            }
            if (pid == -1) {
                logging.warn(
                    "wait3() returned an error: {s}. rdb_child_pid = {}, " ++
                        "aof_child_pid = {}",
                    .{
                        libc.strerror(std.c._errno().*),
                        server.rdb_child_pid,
                        server.aof_child_pid,
                    },
                );
            } else if (pid == server.rdb_child_pid) {
                rdb.backgroundSaveDoneHandler(exitcode, bysignal);
                if (bysignal == 0 and exitcode == 0) {
                    childinfo.receive();
                }
            } else if (pid == server.aof_child_pid) {
                aof.backgroundRewriteDoneHandler(exitcode, bysignal);
                if (bysignal == 0 and exitcode == 0) {
                    childinfo.receive();
                }
            }

            updateDictResizePolicy();
            childinfo.closePipe();
        }
    } else {
        // If there is not a background saving/rewrite in progress check if
        // we have to save/rewrite now.
        for (server.saveparams.items) |*sp| {
            // Save if we reached the given amount of changes,
            // the given amount of seconds, and if the latest bgsave was
            // successful or if, in case of an error, at least
            // CONFIG_BGSAVE_RETRY_DELAY seconds already elapsed.
            if (server.dirty >= sp.changes and
                server.unixtime.get() - server.lastsave > sp.seconds and
                (server.unixtime.get() - server.lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY or
                    server.lastbgsave_status == true))
            {
                logging.notice(
                    "{} changes in {} seconds. Saving...",
                    .{ sp.changes, sp.seconds },
                );
                _ = rdb.saveBackground(server.rdb_filename);
                break;
            }
        }
        // Trigger an AOF rewrite if needed.
        if (server.aof_state == Server.AOF_ON and server.rdb_child_pid == -1 and
            server.aof_child_pid == -1 and server.aof_rewrite_perc != 0 and
            server.aof_current_size > server.aof_rewrite_min_size)
        {
            const base: usize = @max(server.aof_rewrite_base_size, 1);
            const growth = @divFloor(server.aof_current_size * 100, base) - 100;
            if (growth >= server.aof_rewrite_perc) {
                logging.notice(
                    "Starting automatic rewriting of AOF on {}% growth",
                    .{growth},
                );
                _ = aof.rewriteAppendOnlyFileBackground();
            }
        }
    }

    // AOF postponed flush: Try at every cron cycle if the slow fsync
    // completed.
    if (server.aof_flush_postponed_start != 0) {
        aof.flushAppendOnlyFile(false);
    }

    // AOF write errors: in this case we have a buffer to flush as well and
    // clear the AOF error in case of success to make the DB writable again,
    // however to try every second is enough in case of 'hz' is set to
    // an higher frequency.
    if (runWithPeriod(1000) and server.aof_last_write_status == false) {
        aof.flushAppendOnlyFile(false);
    }

    // Close clients that need to be closed asynchronous.
    networking.freeClientsInAsyncFreeQueue();

    // Start a scheduled BGSAVE if the corresponding flag is set. This is
    // useful when we are forced to postpone a BGSAVE because an AOF
    // rewrite is in progress.
    //
    // Note: this code must be after the replicationCron() call above so
    // make sure when refactoring this file to keep this order. This is useful
    // because we want to give priority to RDB savings for replication.
    if (server.rdb_child_pid == -1 and server.aof_child_pid != -1 and
        server.rdb_bgsave_scheduled and
        ((server.unixtime.get() - server.lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY) or
            server.lastbgsave_status == true))
    {
        if (rdb.saveBackground(server.rdb_filename)) {
            server.rdb_bgsave_scheduled = false;
        }
    }

    server.cronloops +%= 1;
    return @intCast(@divFloor(1000, server.hz));
}

pub fn prepareForShutdown(flags: i32) bool {
    const save = flags & SHUTDOWN_SAVE != 0;
    const nosave = flags & SHUTDOWN_NOSAVE != 0;

    logging.warn("User requested shutdown..", .{});

    // Kill the saving child if there is a background saving in progress.
    // We want to avoid race conditions, for instance our saving child may
    // overwrite the synchronous saving did by SHUTDOWN.
    if (server.rdb_child_pid != -1) {
        logging.warn("There is a child saving an .rdb. Killing it!", .{});
        posix.kill(server.rdb_child_pid, posix.SIG.USR1) catch {};
        rdb.rdbRemoveTempFile(server.rdb_child_pid);
    }

    if (server.aof_state != AOF_OFF) {
        // Kill the AOF saving child as the AOF we already have may be longer
        // but contains the full dataset anyway.
        if (server.aof_child_pid != -1) {
            // If we have AOF enabled but haven't written the AOF yet, don't
            // shutdown or else the dataset will be lost.
            if (server.aof_state == AOF_WAIT_REWRITE) {
                logging.warn("Writing initial AOF, can't exit.", .{});
                return false;
            }
            logging.warn(
                "There is a child rewriting the AOF. Killing it!",
                .{},
            );
            posix.kill(server.aof_child_pid, posix.SIG.USR1) catch {};
        }
        // Append only file: flush buffers and fsync() the AOF at exit
        logging.notice("Calling fsync() on the AOF file.", .{});
        aof.flushAppendOnlyFile(true);
        config.fsync(server.aof_fd) catch {};
    }

    // Create a new RDB file before exiting.
    if ((server.saveparams.items.len > 0 and !nosave) or save) {
        logging.notice("Saving the final RDB snapshot before exiting.", .{});
        // Snapshotting. Perform a SYNC SAVE and exit
        if (!rdb.save(server.rdb_filename)) {
            // Ooops.. error saving! The best we can do is to continue
            // operating. Note that if there was a background saving process,
            // in the next cron() Redis will be notified that the background
            // saving aborted, handling special stuff like slaves pending for
            // synchronization...
            logging.warn("Error trying to save the DB, can't exit.", .{});
            return false;
        }
    }

    // Close the listening sockets. Apparently this allows faster restarts.
    closeListeningSockets(true);

    logging.warn("Redis is now ready to exit, bye bye...", .{});
    return true;
}

/// Close listening sockets. Also unlink the unix domain socket if
/// unlink_unix_socket is TRUE.
fn closeListeningSockets(unlink_unix_socket: bool) void {
    for (0..server.ipfd_count) |i| {
        posix.close(server.ipfd[i]);
    }
    if (server.sofd != -1) {
        posix.close(server.sofd);
    }

    if (unlink_unix_socket and server.unixsocket != null) {
        logging.notice("Removing the unix socket file.", .{});
        // don't care if this fails
        posix.unlink(server.unixsocket.?) catch {};
    }
}

/// This function is called by serverCron() and is used in order to perform
/// operations on clients that are important to perform constantly. For instance
/// we use this function in order to disconnect clients after a timeout, including
/// clients blocked in some blocking command with a non-zero timeout.
///
/// The function makes some effort to process all the clients every second, even
/// if this cannot be strictly guaranteed, since serverCron() may be called with
/// an actual frequency lower than server.hz in case of latency events like slow
/// commands.
///
/// It is very important for this function, and the functions it calls, to be
/// very fast: sometimes Redis has tens of hundreds of connected clients, and the
/// default server.hz value is 10, so sometimes here we need to process thousands
/// of clients per second, turning this function into a source of latency.
fn clientsCron() void {
    // Try to process at least numclients/server.hz of clients
    // per call. Since normally (if there are no big latency events) this
    // function is called server.hz times per second, in the average case we
    // process all the clients in 1 second.
    const numclients = server.clients.len;
    var iterations: usize = @divFloor(numclients, server.hz);
    const now_ms = std.time.milliTimestamp();
    const CLIENTS_CRON_MIN_ITERATIONS = 5;

    // Process at least a few clients while we are at it, even if we need
    // to process less than CLIENTS_CRON_MIN_ITERATIONS to meet our contract
    // of processing each client once per second.
    if (iterations < CLIENTS_CRON_MIN_ITERATIONS) {
        iterations = if (numclients < CLIENTS_CRON_MIN_ITERATIONS)
            numclients
        else
            CLIENTS_CRON_MIN_ITERATIONS;
    }
    while (server.clients.len > 0 and iterations > 0) : (iterations -= 1) {
        // Rotate the list, take the current head, process.
        // This way if the client must be removed from the list it's the
        // first element and we don't incur into O(N) computation.
        server.clients.rotateTailToHead();
        const head = server.clients.first.?;
        const cli = head.value;

        // The following functions do different service checks on the client.
        // The protocol is that they return true if the client was
        // terminated.
        if (clientsCronHandleTimeout(cli, now_ms)) continue;
        if (clientsCronResizeQueryBuffer(cli)) continue;
    }
}

/// Check for timeouts. Returns true if the client was terminated.
fn clientsCronHandleTimeout(cli: *Client, now_ms: i64) bool {
    const now_sec: i64 = @divFloor(now_ms, std.time.ms_per_s);
    if (server.maxidletime > 0 and
        cli.flags & CLIENT_BLOCKED == 0 and // no timeout for BLPOP
        cli.flags & CLIENT_PUBSUB == 0 and // no timeout for Pub/Sub clients
        (now_sec - cli.lastinteraction > server.maxidletime))
    {
        logging.verbose("Closing idle client", .{});
        cli.free();
        return true;
    } else if (cli.flags & CLIENT_BLOCKED != 0) {
        // Blocked OPS timeout is handled with milliseconds resolution.
        // However note that the actual resolution is limited by
        // server.hz.
        if (cli.bpop.timeout != 0 and cli.bpop.timeout < now_ms) {
            // Handle blocking operation specific timeout.
            blocked.replyToBlockedClientTimedOut(cli);
            blocked.unblockClient(cli);
        }
    }
    return false;
}

/// The client query buffer is an sds.String that can end with a lot of
/// free space not used, this function reclaims space if needed.
///
/// The function always returns false as it never terminates the client.
fn clientsCronResizeQueryBuffer(cli: *Client) bool {
    const querybuf_size = sds.getAlloc(cli.querybuf);
    const idletime = server.unixtime.get() - cli.lastinteraction;

    // There are two conditions to resize the query buffer:
    // 1) Query buffer is > BIG_ARG and too big for latest peak.
    // 2) Query buffer is > BIG_ARG and client is idle.
    if (querybuf_size > PROTO_MBULK_BIG_ARG and
        (@divFloor(querybuf_size, cli.querybuf_peak + 1) > 2 or idletime > 2))
    {
        // Only resize the query buffer if it is actually wasting
        // at least a few kbytes.
        if (sds.getAvail(cli.querybuf) > 4 * 1024) {
            cli.querybuf = sds.removeAvailSpace(
                allocator.impl,
                cli.querybuf,
            );
        }
    }

    // Reset the peak again to capture the peak memory usage in the next
    // cycle
    cli.querybuf_peak = 0;

    return false;
}

/// This function handles 'background' operations we are required to do
/// incrementally in Redis databases, such as active key expiring, resizing,
/// rehashing.
fn databaseCron() void {
    // Expire keys by random sampling.
    if (server.active_expire_enabled) {
        expire.activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
    }

    // Perform hash tables rehashing if needed, but only if there are no
    // other processes saving the DB on disk. Otherwise rehashing is bad
    // as will cause a lot of copy-on-write of memory pages.
    if (server.rdb_child_pid == -1 and server.aof_child_pid == -1) {
        // We use global counters so if we stop the computation at a given
        // DB we'll be able to start from the successive in the next
        // cron loop iteration.
        const State = struct {
            var resize_db: usize = 0;
            var rehash_db: usize = 0;
        };
        // Don't test more DBs than we have.
        const dbs_per_call = @min(CRON_DBS_PER_CALL, server.dbnum);

        // Resize
        for (0..dbs_per_call) |_| {
            tryResizeHashTables(@rem(State.resize_db, server.dbnum));
            State.resize_db +%= 1;
        }

        // Rehash
        if (server.activerehashing) {
            for (0..dbs_per_call) |_| {
                const work_done = incrementallyRehash(State.rehash_db);
                if (work_done) {
                    // If the function did some work, stop here, we'll do
                    // more at the next cron loop.
                    break;
                } else {
                    // If this db didn't need rehash, we'll try the next one.
                    State.rehash_db += 1;
                    State.rehash_db = @rem(State.rehash_db, server.dbnum);
                }
            }
        }
    }
}

/// If the percentage of used slots in the HT reaches HASHTABLE_MIN_FILL
/// we resize the hash table to save memory
fn tryResizeHashTables(dbid: usize) void {
    const db = &server.db[dbid];
    if (needShrinkDictToFit(db.dict.size(), db.dict.slots())) {
        _ = db.dict.shrinkToFit();
    }
    if (needShrinkDictToFit(db.expires.size(), db.expires.slots())) {
        _ = db.expires.shrinkToFit();
    }
}

/// Our hash table implementation performs rehashing incrementally while
/// we write/read from the hash table. Still if the server is idle, the hash
/// table will use two tables for a long time. So we try to use 1 millisecond
/// of CPU time at every call of this function to perform some rehahsing.
///
/// The function returns TRUE if some rehashing was performed, otherwise FALSE
/// is returned.
fn incrementallyRehash(dbid: usize) bool {
    const db = &server.db[dbid];
    if (db.dict.isRehashing()) {
        db.dict.rehashMilliseconds(1);
        return true; // already used our millisecond for this loop...
    }
    if (db.expires.isRehashing()) {
        db.expires.rehashMilliseconds(1);
        return true; // already used our millisecond for this loop...
    }
    return false;
}

/// If this function gets called we already read a whole
/// command, arguments are in the client argv/argc fields.
/// processCommand() execute the command or prepare the
/// server for a bulk read from the client.
///
/// If true is returned the client is still alive and valid and
/// other operations can be performed by the caller. Otherwise
/// if false is returned the client was destroyed (i.e. after QUIT).
pub fn processCommand(self: *Server, cli: *Client) bool {
    const cmd: sds.String = sds.cast(cli.argv.?[0].v.ptr);
    if (std.ascii.eqlIgnoreCase(sds.asBytes(cmd), "quit")) {
        cli.addReply(shared.ok);
        cli.flags |= CLIENT_CLOSE_AFTER_REPLY;
        return false;
    }

    // Now lookup the command and check ASAP about trivial error conditions
    // such as wrong arity, bad command name and so forth.
    cli.cmd = self.lookupCommand(cmd);
    cli.lastcmd = cli.cmd;
    if (cli.cmd == null) {
        multi.flagTransaction(cli);
        var args = sds.empty(allocator.impl);
        defer sds.free(allocator.impl, args);
        var i: usize = 1;
        while (i < cli.argc and sds.getLen(args) < 128) : (i += 1) {
            const arg: sds.String = @ptrCast(cli.argv.?[i].v.ptr);
            var remaining = 128 - sds.getLen(args);
            if (remaining > sds.getLen(arg)) {
                remaining = sds.getLen(arg);
            }
            args = sds.catPrintf(
                allocator.impl,
                args,
                "`{s}`, ",
                .{arg[0..remaining]},
            );
        }
        cli.addReplyErrFormat(
            "unknown command: `{s}`, with args beginning with: {s}",
            .{
                sds.asBytes(cmd),
                sds.asBytes(args),
            },
        );
        return true;
    }
    const arity = cli.cmd.?.arity;
    if ((arity > 0 and arity != cli.argc) or (cli.argc < -arity)) {
        multi.flagTransaction(cli);
        cli.addReplyErrFormat(
            "wrong number of arguments for '{s}' command",
            .{cli.cmd.?.name},
        );
        return true;
    }

    // Check if the user is authenticated
    if (self.requirepass != null and !cli.authenticated and
        cli.cmd.?.proc != authCommand)
    {
        multi.flagTransaction(cli);
        cli.addReply(shared.noautherr);
        return true;
    }

    // Only allow SUBSCRIBE and UNSUBSCRIBE in the context of Pub/Sub
    if (cli.flags & CLIENT_PUBSUB != 0 and
        cli.cmd.?.proc != pingCommand and
        cli.cmd.?.proc != pubsub.subscribeCommand and
        cli.cmd.?.proc != pubsub.unsubscribeCommand and
        cli.cmd.?.proc != pubsub.psubscribeCommand and
        cli.cmd.?.proc != pubsub.punsubscribeCommand)
    {
        cli.addReplyErr("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING " ++
            "/ QUIT allowed in this context");
        return true;
    }

    // Loading DB? Return an error if the command has not the
    // CMD_LOADING flag.
    if (self.loading and cli.cmd.?.flags & CMD_LOADING == 0) {
        cli.addReply(Server.shared.loadingerr);
        return true;
    }

    // Lua script too slow? Only allow a limited number of commands.
    if (server.lua.timeout and cli.cmd.?.proc != authCommand and
        !(cli.cmd.?.proc == dbt.shutdownCommand and
            cli.argc == 2 and
            toLower(sds.castBytes(cli.argv.?[1].v.ptr)[0]) == 'n') and
        !(cli.cmd.?.proc == scripting.scriptCommand and
            cli.argc == 2 and
            toLower(sds.castBytes(cli.argv.?[1].v.ptr)[0]) == 'k'))
    {
        multi.flagTransaction(cli);
        cli.addReply(shared.slowscripterr);
        return true;
    }

    // Exec the command
    if (cli.flags & CLIENT_MULTI != 0 and
        cli.cmd.?.proc != multi.execCommand and
        cli.cmd.?.proc != multi.discardCommand and
        cli.cmd.?.proc != multi.multiCommand and
        cli.cmd.?.proc != multi.watchCommand)
    {
        multi.queueMultiCommand(cli);
        cli.addReply(Server.shared.queued);
    } else {
        self.call(cli, CMD_CALL_FULL);
        if (self.ready_keys.len > 0) {
            blocked.handleClientsBlockedOnKeys();
        }
    }

    return true;
}

pub fn lookupCommand(self: *Server, cmd: sds.String) ?*Command {
    const command = self.commands.fetchValue(cmd) orelse {
        return null;
    };
    return command;
}

pub fn lookupCommandByString(self: *Server, cmd: []const u8) *Command {
    const name = sds.new(allocator.impl, cmd);
    defer sds.free(allocator.impl, name);
    return self.commands.fetchValue(name).?;
}

/// Lookup the command in the current table, if not found also check in
/// the original table containing the original command names unaffected by
/// redis.conf rename-command statement.
///
/// This is used by functions rewriting the argument vector such as
/// rewriteClientCommandVector() in order to set client->cmd pointer
/// correctly even if the command was renamed.
pub fn lookupCommandOrOriginal(self: *Server, name: sds.String) ?*Command {
    const cmd = self.commands.fetchValue(name) orelse {
        // TODO: fetch from original commands
        return null;
    };
    return cmd;
}

fn populateCommandTable(self: *Server) void {
    for (0..commandtable.table.len) |i| {
        const command = &commandtable.table[i];
        for (command.sflags) |f| {
            switch (f) {
                'w' => command.flags |= Server.CMD_WRITE,
                'r' => command.flags |= Server.CMD_READONLY,
                'm' => command.flags |= Server.CMD_DENYOOM,
                'a' => command.flags |= Server.CMD_ADMIN,
                'p' => command.flags |= Server.CMD_PUBSUB,
                's' => command.flags |= Server.CMD_NOSCRIPT,
                'R' => command.flags |= Server.CMD_RANDOM,
                'S' => command.flags |= Server.CMD_SORT_FOR_SCRIPT,
                'l' => command.flags |= Server.CMD_LOADING,
                't' => command.flags |= Server.CMD_STALE,
                'M' => command.flags |= Server.CMD_SKIP_MONITOR,
                'k' => command.flags |= Server.CMD_ASKING,
                'F' => command.flags |= Server.CMD_FAST,
                else => @panic("Unsupported command flag"),
            }
        }
        assert(self.commands.add(
            sds.new(allocator.impl, command.name),
            @constCast(command),
        ));
    }
}

/// call() is the core of Redis execution of a command.
///
/// The following flags can be passed:
/// CMD_CALL_NONE        No flags.
/// CMD_CALL_SLOWLOG     Check command speed and log in the slow log if needed.
/// CMD_CALL_STATS       Populate command stats.
/// CMD_CALL_PROPAGATE_AOF   Append command to AOF if it modified the dataset
///                          or if the client flags are forcing propagation.
/// CMD_CALL_PROPAGATE_REPL  Send command to salves if it modified the dataset
///                          or if the client flags are forcing propagation.
/// CMD_CALL_PROPAGATE   Alias for PROPAGATE_AOF|PROPAGATE_REPL.
/// CMD_CALL_FULL        Alias for SLOWLOG|STATS|PROPAGATE.
///
/// The exact propagation behavior depends on the client flags.
/// Specifically:
///
/// 1. If the client flags CLIENT_FORCE_AOF or CLIENT_FORCE_REPL are set
///    and assuming the corresponding CMD_CALL_PROPAGATE_AOF/REPL is set
///    in the call flags, then the command is propagated even if the
///    dataset was not affected by the command.
/// 2. If the client flags CLIENT_PREVENT_REPL_PROP or CLIENT_PREVENT_AOF_PROP
///    are set, the propagation into AOF or to slaves is not performed even
///    if the command modified the dataset.
///
/// Note that regardless of the client flags, if CMD_CALL_PROPAGATE_AOF
/// or CMD_CALL_PROPAGATE_REPL are not set, then respectively AOF or
/// slaves propagation will never occur.
///
/// Client flags are modified by the implementation of a given command
/// using the following API:
///
/// forceCommandPropagation(Client *cli, flags: i32);
/// preventCommandPropagation(Client *cli);
/// preventCommandAOF(Client *cli);
pub fn call(self: *Server, cli: *Client, flags: i32) void {
    const clien_old_flags = cli.flags;
    self.fixed_time_expire +%= 1;

    // Initialization: clear the flags that must be set by the command on
    // demand, and initialize the array for additional commands propagation.
    cli.flags &= ~@as(i32, CLIENT_FORCE_AOF | CLIENT_PREVENT_PROP);
    const prev_also_propagate = self.also_prpagate;
    self.also_prpagate.init();

    // Call the command
    var dirty = self.dirty;
    self.updateCachedTime();
    cli.cmd.?.proc(cli);
    dirty = self.dirty -% dirty;
    if (dirty < 0) dirty = 0;

    // If the caller is Lua, we want to force the EVAL caller to propagate
    // the script if the command flag or client flag are forcing the
    // propagation.
    if (cli.flags & CLIENT_LUA != 0 and self.lua.caller != null) {
        if (cli.flags & CLIENT_FORCE_AOF != 0) {
            self.lua.caller.?.flags |= CLIENT_FORCE_AOF;
        }
    }

    // Propagate the command into the AOF and replication link
    if (flags & CMD_CALL_PROPAGATE != 0 and
        cli.flags & CLIENT_PREVENT_PROP != CLIENT_PREVENT_PROP)
    {
        var propagate_flags: i32 = PROPAGATE_NONE;

        // Check if the command operated changes in the data set. If so
        // set for replication / AOF propagation.
        if (dirty > 0) {
            propagate_flags |= (PROPAGATE_AOF | PROPAGATE_REPL);
        }

        // If the client forced AOF / replication of the command, set
        // the flags regardless of the command effects on the data set.
        if (cli.flags & CLIENT_FORCE_AOF != 0) {
            propagate_flags |= PROPAGATE_AOF;
        }

        // However prevent AOF / replication propagation if the command
        // implementations called preventCommandPropagation() or similar,
        // or if we don't have the call() flags to do so.
        if (cli.flags & CLIENT_PREVENT_AOF_PROP != 0 or
            flags & CMD_CALL_PROPAGATE_AOF == 0)
        {
            propagate_flags &= @as(i32, PROPAGATE_AOF);
        }

        // Call propagate() only if at least one of AOF / replication
        // propagation is needed.
        if (propagate_flags != PROPAGATE_NONE) {
            self.propagate(
                cli.cmd.?,
                cli.db.id,
                cli.argv.?,
                cli.argc,
                propagate_flags,
            );
        }
    }

    // Restore the old  flags, since call() can be executed recursively.
    cli.flags &= ~@as(i32, CLIENT_FORCE_AOF);
    cli.flags |= clien_old_flags & (CLIENT_FORCE_AOF | CLIENT_PREVENT_PROP);

    // Handle the alsoPropagate() API to handle commands that want to propagate
    // multiple separated commands. Note that alsoPropagate() is not affected
    // by CLIENT_PREVENT_PROP flag.
    if (self.also_prpagate.ops) |ops| {
        if (flags & CMD_CALL_PROPAGATE != 0) {
            for (ops) |*op| {
                var target = op.target;
                if (flags & CMD_CALL_PROPAGATE_AOF == 0) {
                    target &= ~@as(i32, PROPAGATE_AOF);
                }
                if (target != 0) {
                    self.propagate(
                        op.cmd,
                        op.dbid,
                        op.argv,
                        op.argc,
                        target,
                    );
                }
            }
        }
        server.also_prpagate.free();
    }
    self.also_prpagate = prev_also_propagate;

    self.fixed_time_expire -%= 1;
}

/// Propagate the specified command (in the context of the specified database id)
/// to AOF and Slaves.
///
/// flags are an xor between:
/// + PROPAGATE_NONE (no propagation of command at all)
/// + PROPAGATE_AOF (propagate into the AOF file if is enabled)
/// + PROPAGATE_REPL (propagate into the replication link)
///
/// This should not be used inside commands implementation since it will not
/// wrap the resulting commands in MULTI/EXEC. Use instead:
///     alsoPropagate()
///     preventCommandPropagation()
///     forceCommandPropagation()
///
/// However for functions that need to (also) propagate out of the context of a
/// command execution, for example when serving a blocked client, you
/// want to use propagate().
pub fn propagate(
    self: *Server,
    cmd: *const Command,
    dbid: usize,
    argv: []const *Object,
    argc: usize,
    flags: i32,
) void {
    if (self.aof_state != AOF_OFF and flags & PROPAGATE_AOF != 0) {
        aof.feedAppendOnlyFile(cmd, @intCast(dbid), argv, argc);
    }
}

/// Used inside commands to schedule the propagation of additional commands
/// after the current command is propagated to AOF / Replication.
///
/// 'cmd' must be a pointer to the Redis command to replicate, dbid is the
/// database ID the command should be propagated into.
/// Arguments of the command to propagte are passed as an array of redis
/// objects pointers of len 'argc', using the 'argv' vector.
///
/// The function does not take a reference to the passed 'argv' vector,
/// so it is up to the caller to release the passed argv (but it is usually
/// stack allocated).  The function autoamtically increments ref count of
/// passed objects, so the caller does not need to.
pub fn alsoPropagate(
    self: *Server,
    cmd: *const Command,
    dbid: usize,
    argv: []const *Object,
    argc: usize,
    target: i32,
) void {
    // No propagation during loading.
    if (self.loading) {
        return;
    }

    const argvcopy = allocator.alloc(*Object, argc);
    for (argv[0..argc], 0..) |arg, i| {
        argvcopy[i] = arg.incrRefCount();
    }
    self.also_prpagate.append(
        cmd,
        dbid,
        argvcopy,
        argvcopy.len,
        target,
    );
}

/// Avoid that the executed command is propagated at all. This way we
/// are free to just propagate what we want using the alsoPropagate()
/// API.
pub fn preventCommandPropagation(cli: *Client) void {
    cli.flags |= CLIENT_PREVENT_PROP;
}

/// It is possible to call the function forceCommandPropagation() inside a
/// Redis command implementation in order to to force the propagation of a
/// specific command execution into AOF.
pub fn forceCommandPropagation(cli: *Client, flags: i32) void {
    if (flags & PROPAGATE_AOF != 0) {
        cli.flags |= CLIENT_FORCE_AOF;
    }
}

// AUTH password
pub fn authCommand(cli: *Client) void {
    if (server.requirepass == null) {
        cli.addReplyErrFormat(
            "Client sent AUTH, but no password is set",
            .{},
        );
        return;
    }
    const password = sds.cast(cli.argv.?[1].v.ptr);
    if (util.timeIndependentEql(
        CONFIG_AUTHPASS_MAX_LEN,
        sds.asBytes(password),
        server.requirepass.?,
    )) {
        cli.authenticated = true;
        cli.addReply(shared.ok);
        return;
    }
    cli.authenticated = false;
    cli.addReplyErr("invalid password");
}

/// The PING command. It works in a different way if the client is in
/// in Pub/Sub mode.
pub fn pingCommand(cli: *Client) void {
    // The command takes zero or one arguments.
    if (cli.argc > 2) {
        cli.addReplyErrFormat(
            "wrong number of arguments for '{s}' command",
            .{cli.cmd.?.name},
        );
        return;
    }

    if (cli.flags & CLIENT_PUBSUB != 0) {
        cli.addReply(shared.mbulkhdr[2]);
        cli.addReplyBulkString("pong");
        if (cli.argc == 1) {
            cli.addReplyBulkString("");
        } else {
            cli.addReplyBulk(cli.argv.?[1]);
        }
    } else {
        if (cli.argc == 1) {
            cli.addReply(shared.pong);
        } else {
            cli.addReplyBulk(cli.argv.?[1]);
        }
    }
}

/// ECHO message
pub fn echoCommand(cli: *Client) void {
    cli.addReplyBulk(cli.argv.?[1]);
}

/// TIME
pub fn timeCommand(cli: *Client) void {
    const ustime = std.time.microTimestamp();
    const sec = @divFloor(ustime, std.time.us_per_s);
    const usec = @rem(ustime, std.time.us_per_s);
    cli.addReplyMultiBulkLen(2);
    cli.addReplyBulkLongLong(sec);
    cli.addReplyBulkLongLong(usec);
}

pub fn needShrinkDictToFit(used: u64, size: u64) bool {
    return (size > dict.HT_INITIAL_SIZE) and
        @divFloor(used *| 100, size) < HASHTABLE_MIN_FILL;
}

/// This function is called once a background process of some kind terminates,
/// as we want to avoid resizing the hash tables when there is a child in order
/// to play well with copy-on-write (otherwise when a resize happens lots of
/// memory pages are copied). The goal of this function is to update the ability
/// for dict.zig to resize the hash tables accordingly to the fact we have o not
/// running childs.
pub fn updateDictResizePolicy() void {
    if (server.rdb_child_pid == -1 and server.aof_child_pid == -1) {
        dict.enableResize();
    } else {
        dict.disableResize();
    }
}

pub fn setProcTitle(title: []const u8) void {
    const mode: []const u8 = "";
    var buf: [255:0]u8 = undefined;
    const s = std.fmt.bufPrint(
        &buf,
        "{s} {s}:{}{s}",
        .{
            title,
            if (server.bindaddr_count > 0) server.bindaddr[0] else "*",
            server.port,
            mode,
        },
    ) catch |err| {
        std.debug.panic("Error: formart proc title: {}", .{err});
    };
    buf[s.len] = 0;
    _ = posix.prctl(
        posix.PR.SET_NAME,
        .{ @intFromPtr(&buf), 0, 0, 0 },
    ) catch |err| {
        logging.warn("Error set process title to '{s}': {}", .{ s, err });
    };
}

/// After fork, the child process will inherit the resources
/// of the parent process, e.g. fd(socket or flock) etc.
/// should close the resources not used by the child process, so that if the
/// parent restarts it can bind/lock despite the child possibly still running.
pub fn closeChildUnusedResourceAfterFork() void {
    closeListeningSockets(false);
    // TODO: pidfile
}

/// After an RDB dump or AOF rewrite we exit from children.
pub fn exitFromChild(retcode: u8) void {
    std.process.exit(retcode);
}

/// Using this function you can run code inside serverCron() with the
/// specified period, specified in milliseconds.
/// The actual resolution depends on server.hz.
inline fn runWithPeriod(ms: i64) bool {
    return ms <= @divFloor(1000, server.hz) or
        @rem(
            server.cronloops,
            @divFloor(ms, @divFloor(1000, server.hz)),
        ) == 0;
}

pub var shared: Object.Shared = undefined;

pub var instance: Server = undefined;

const Commands = struct {
    const HashMap = dict.Dict(sds.String, *Command);

    const vtable: *const HashMap.VTable = &.{
        .hash = hash,
        .eql = sds.eqlCase,
        .freeKey = freeKey,
    };

    fn hash(key: sds.String) hasher.Hash {
        return hasher.hashcase(sds.asBytes(key), 1024);
    }

    fn freeKey(key: sds.String) void {
        sds.free(allocator.impl, key);
    }
};

pub const ClientList = List(*Client, *Client);
const ReadyKeys = List(*blocked.ReadyList, *blocked.ReadyList);

/// Defines an array of Redis operations. There is an API to add to this
/// structure in a easy way.
///
/// redisOpArrayInit();
/// redisOpArrayAppend();
/// redisOpArrayFree();
pub const RedisOpArray = struct {
    /// The redisOp structure defines a Redis Operation, that is an instance of
    /// a command with an argument vector, database ID, propagation target
    /// (PROPAGATE_*), and command pointer.
    ///
    /// Currently only used to additionally propagate more commands to
    /// AOF/Replication after the propagation of the executed command.
    pub const RedisOp = struct {
        argv: []*Object,
        argc: usize,
        dbid: usize,
        target: i32,
        cmd: *const Command,
    };

    ops: ?[]RedisOp,

    pub fn init(self: *RedisOpArray) void {
        self.ops = null;
    }

    pub fn append(
        self: *RedisOpArray,
        cmd: *const Command,
        dbid: usize,
        argv: []*Object,
        argc: usize,
        target: i32,
    ) void {
        const numops = if (self.ops) |ops| ops.len else 0;
        self.ops = if (self.ops != null)
            allocator.alloc(RedisOp, 1)
        else
            allocator.realloc(self.ops.?, self.ops.?.len + 1);
        self.ops.?[numops] = .{
            .argv = argv,
            .argc = argc,
            .dbid = dbid,
            .target = target,
            .cmd = cmd,
        };
    }

    pub fn free(self: *RedisOpArray) void {
        if (self.ops) |ops| {
            for (ops) |op| {
                for (0..op.argc) |i| {
                    op.argv[i].decrRefCount();
                }
                allocator.free(op.argv);
            }
            allocator.free(ops);
        }
    }
};

pub const SaveParam = struct {
    seconds: i32,
    changes: i32,
};

test {
    _ = @import("sds.zig");
    _ = @import("IntSet.zig");
    _ = @import("Object.zig");
    _ = @import("QuickList.zig");
    _ = @import("ZipList.zig");
    _ = @import("ae.zig");
    _ = @import("config.zig");
    _ = @import("dict.zig");
    _ = @import("list.zig");
    _ = @import("t_zset.zig");
    _ = @import("util.zig");
}

const server = &instance;
const Object = @import("Object.zig");
const std = @import("std");
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
const random = @import("random.zig");
const config = @import("config.zig");
const ae = @import("ae.zig");
const util = @import("util.zig");
const List = @import("list.zig").List;
const networking = @import("networking.zig");
const anet = @import("anet.zig");
const evict = @import("evict.zig");
const posix = std.posix;
const atomic = @import("atomic.zig");
const Client = networking.Client;
const Database = @import("db.zig").Database;
const raxlib = @import("rax.zig").rax;
const bio = @import("bio.zig");
const commandtable = @import("commandtable.zig");
pub const Command = commandtable.Command;
pub const blocked = @import("blocked.zig");
const dict = @import("dict.zig");
const libc = @cImport({
    @cInclude("sys/signal.h");
    @cInclude("sys/wait.h");
    @cInclude("sys/errno.h");
    @cInclude("string.h");
    @cInclude("unistd.h");
});
const assert = std.debug.assert;
const hasher = @import("hasher.zig");
const pubsub = @import("pubsub.zig");
const multi = @import("multi.zig");
const zlua = @import("zlua");
const scripting = @import("scripting.zig");
const logging = @import("logging.zig");
const rdb = @import("rdb.zig");
const childinfo = @import("childinfo.zig");
const aof = @import("aof.zig");
const expire = @import("expire.zig");
const dbt = @import("db.zig");
const toLower = std.ascii.toLower;
