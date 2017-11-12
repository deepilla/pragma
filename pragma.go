package pragma

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	applicationID           = "application_id"
	autoVacuum              = "auto_vacuum"
	automaticIndex          = "automatic_index"
	busytimeout             = "busy_timeout"
	cacheSize               = "cache_size"
	cacheSpill              = "cache_spill"
	caseSensitiveLike       = "case_sensitive_like"
	cellSizeCheck           = "cell_size_check"
	collationList           = "collation_list"
	compileOptions          = "compile_options"
	dataVersion             = "data_version"
	databaseList            = "database_list"
	deferForeignKeys        = "defer_foreign_keys"
	encoding                = "encoding"
	foreignKeyCheck         = "foreign_key_check"
	foreignKeyList          = "foreign_key_list"
	foreignKeys             = "foreign_keys"
	freelistCount           = "freelist_count"
	fullfsync               = "fullfsync"
	fullfsyncCheckpoint     = "checkpoint_fullfsync"
	incrementalVacuum       = "incremental_vacuum"
	indexInfo               = "index_info"
	indexList               = "index_list"
	indexInfoAux            = "index_xinfo"
	ignoreCheckConstraints  = "ignore_check_constraints"
	integrityCheck          = "integrity_check"
	journalMode             = "journal_mode"
	journalSizeLimit        = "journal_size_limit"
	legacyFileFormat        = "legacy_file_format"
	lockingMode             = "locking_mode"
	maxPageCount            = "max_page_count"
	memoryMapSize           = "mmap_size"
	pageCount               = "page_count"
	pageSize                = "page_size"
	quickCheck              = "quick_check"
	readOnly                = "query_only"
	readUncommitted         = "read_uncommitted"
	recursiveTriggers       = "recursive_triggers"
	reverseUnorderedSelects = "reverse_unordered_selects"
	secureDelete            = "secure_delete"
	softHeapLimit           = "soft_heap_limit"
	shrinkMemory            = "shrink_memory"
	synchronous             = "synchronous"
	tableInfo               = "table_info"
	tempStore               = "temp_store"
	threads                 = "threads"
	userVersion             = "user_version"
	walAutoCheckpoint       = "wal_autocheckpoint"
	walCheckpoint           = "wal_checkpoint"
)

// A Schema represents a database attached to the current
// database connection.
type Schema struct {
	name string
}

// DB returns a Schema with the given name. It does not verify
// that a database with this name actually exists.
func DB(name string) *Schema {
	return &Schema{name}
}

var (
	// Main is the database used to open a database connection.
	Main = DB("main")

	// Temp is the database used to store temporary tables and
	// indexes. It is not guaranteed to exist.
	Temp = DB("temp")
)

var noSchema = &Schema{}

// Properties
// TODO: Find a way to generate this code!

// ApplicationID returns the application-defined identifier
// from the header of the main database file.
func ApplicationID(db *sql.DB) (int64, error) {
	return noSchema.ApplicationID(db)
}

// ApplicationID returns the application-defined identifier
// from the header of this Schema's database file.
func (s *Schema) ApplicationID(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, applicationID)
}

// SetApplicationID sets the application-defined identifier
// in the header of the main database file.
func SetApplicationID(db *sql.DB, id int64) error {
	return noSchema.SetApplicationID(db, id)
}

// SetApplicationID sets the application-defined identifier
// in the header of this Schema's database file.
func (s *Schema) SetApplicationID(db *sql.DB, id int64) error {
	return s.setPragmaInt(db, applicationID, id)
}

// AutoVacuumValue represents SQLite's auto vacuum setting.
type AutoVacuumValue uint

const (
	// AutoVacuumNone means that auto vacuum is disabled. When
	// content is deleted from the database, the disk space is
	// added to the freelist for reuse instead of being freed.
	// This is the default behaviour (unless changed with a
	// compile-time option).
	AutoVacuumNone AutoVacuumValue = iota

	// AutoVacuumFull means that auto vacuum is enabled. When
	// content is deleted from the database, the disk space is
	// freed immediately.
	AutoVacuumFull

	// AutoVacuumIncremental means that auto vacuum is partly
	// enabled. When content is deleted from the database,
	// SQLite prepares to free the disk space but doesn't
	// do it. Disk space is freed manually by calling
	// RunIncrementalVacuum.
	AutoVacuumIncremental
)

// Scan implements the sql.Scanner interface.
func (v *AutoVacuumValue) Scan(src interface{}) error {

	i, ok := toInt(src)
	if !ok {
		return fmt.Errorf("invalid AutoVacuumValue: %T %v", src, src)
	}

	switch i {
	case 0:
		*v = AutoVacuumNone
	case 1:
		*v = AutoVacuumFull
	case 2:
		*v = AutoVacuumIncremental
	default:
		return fmt.Errorf("unexpected AutoVacuumValue: %d", i)
	}

	return nil
}

func (v AutoVacuumValue) value() (interface{}, error) {
	switch v {
	case AutoVacuumNone, AutoVacuumFull, AutoVacuumIncremental:
		return int64(v), nil
	default:
		return nil, fmt.Errorf("unexpected AutoVacuumValue: %v", v)
	}
}

// AutoVacuum returns the auto vacuum status of the main database.
func AutoVacuum(db *sql.DB) (AutoVacuumValue, error) {
	return noSchema.AutoVacuum(db)
}

// AutoVacuum returns the auto vacuum status of this Schema.
func (s *Schema) AutoVacuum(db *sql.DB) (AutoVacuumValue, error) {
	var dest AutoVacuumValue
	return dest, s.pragma(db, autoVacuum, &dest)
}

// SetAutoVacuum sets the auto vacuum status of the main database.
func SetAutoVacuum(db *sql.DB, value AutoVacuumValue) error {
	return noSchema.SetAutoVacuum(db, value)
}

// SetAutoVacuum sets the auto vacuum status of this Schema.
func (s *Schema) SetAutoVacuum(db *sql.DB, value AutoVacuumValue) error {
	return s.setPragmaValue(db, autoVacuum, value)
}

// AutoIndex returns a boolean indicating whether automatic
// indexing capability is enabled. With automatic indexes
// enabled (the default), SQLite will create temporary indexes
// on tables to improve query performance.
func AutoIndex(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, automaticIndex)
}

// SetAutoIndex enables or disables sqlite's automatic indexing
// capability. With automatic indexes enabled (the default),
// SQLite will create temporary indexes on tables to improve
// query performance.
func SetAutoIndex(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, automaticIndex, on)
}

// BusyTimeout returns the maximum number of milliseconds that
// SQLite will wait before timing out when trying to access a
// table that is locked by another process. The default is zero.
func BusyTimeout(db *sql.DB) (int64, error) {
	return noSchema.pragmaInt(db, busytimeout)
}

// SetBusyTimeout sets the maximum number of milliseconds that
// SQLite will wait before timing out when trying to access a
// table that is locked by another process. Pass zero to clear
// the existing timeout.
func SetBusyTimeout(db *sql.DB, ms int64) error {
	return noSchema.setPragmaInt(db, busytimeout, ms)
}

// CacheSizeUnit is a unit of measurement used to express SQLite
// cache size.
type CacheSizeUnit uint

// The SQLite cache size is always a whole number of database
// pages. For convenience, SQLite allows you to define it as an
// amount of memory in kibibytes (1 KiB = 1024 bytes), in which
// case the cache is adjusted to use the desired amount of memory.
const (
	// CacheSizePages denotes a cache size expressed in pages.
	CacheSizePages CacheSizeUnit = iota

	// CacheSizeKiB denotes a cache size expressed in kibibytes.
	CacheSizeKiB
)

// CacheSize returns the maximum number of main database pages
// that SQLite can hold in memory at one time. The default cache
// size is 2000 KiB (unless overridden by a compile-time option).
func CacheSize(db *sql.DB) (int64, CacheSizeUnit, error) {
	return noSchema.CacheSize(db)
}

// CacheSize returns the maximum number of pages of this Schema's
// database that SQLite can hold in memory at one time. The default
// cache size for non-temporary databases is 2000 KiB (unless
// overridden by a compile-time option).
func (s *Schema) CacheSize(db *sql.DB) (int64, CacheSizeUnit, error) {

	n, err := s.pragmaInt(db, cacheSize)
	if err != nil {
		return 0, CacheSizePages, err
	}

	if n < 0 {
		return abs(n), CacheSizeKiB, nil
	}

	return n, CacheSizePages, nil
}

// SetCacheSize sets the maximum number of main database pages
// that SQLite can hold in memory at one time.
//
// Note that this setting only lasts for the current database
// session.
func SetCacheSize(db *sql.DB, value int64, unit CacheSizeUnit) error {
	return noSchema.SetCacheSize(db, value, unit)
}

// SetCacheSize sets the maximum number of pages from this
// Schema's database that SQLite can hold in memory at one time.
//
// Note that this setting only lasts for the current database
// session.
func (s *Schema) SetCacheSize(db *sql.DB, value int64, unit CacheSizeUnit) error {

	switch unit {
	case CacheSizePages:
		value = abs(value)
	case CacheSizeKiB:
		value = abs(value) * -1
	default:
		return fmt.Errorf("unexpected CacheSizeUnit %v", unit)
	}

	return s.setPragmaInt(db, cacheSize, value)
}

// CacheSpill returns a boolean indicating whether SQLite is able
// to spill dirty cache pages to database files mid-transaction.
func CacheSpill(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, cacheSpill)
}

// SetCacheSpill enables or disables cache spill. With cache spill
// on, SQLite is able to spill dirty cache pages to database files
// mid-transaction.
func SetCacheSpill(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, cacheSpill, on)
}

// SetCaseSensitiveLike enables or disables case sensitivity in
// LIKE clauses. By default, LIKE clauses in SQLite are not case
// sensitive, e.g. 'A' LIKE 'a' evaluates to True.
func SetCaseSensitiveLike(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, caseSensitiveLike, on)
}

// CellSizeChecks returns a boolean indicating the status of cell
// size checking. Cell size checks are additional sanity checks
// on database pages as they are read from disk. When enabled,
// database corruption is detected earlier. But there is a small
// performance hit so the checks are disabled by default.
func CellSizeChecks(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, cellSizeCheck)
}

// SetCellSizeChecks enables or disables additional sanity checks
// as database pages are read from disk. When enabled, database
// corruption is detected earlier but there is a small performance
// hit.
func SetCellSizeChecks(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, cellSizeCheck, on)
}

// CheckConstraints returns a boolean indicating whether CHECK
// constraints are being enforced.
func CheckConstraints(db *sql.DB) (bool, error) {
	b, err := noSchema.pragmaBool(db, ignoreCheckConstraints)
	if err != nil {
		return false, err
	}
	return !b, nil
}

// SetCheckConstraints enables or disables enforcing of CHECK
// constraints.
func SetCheckConstraints(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, ignoreCheckConstraints, !on)
}

// DataVersion provides a way to detect changes to the main
// database file by other processes. Consecutive calls to
// DataVersion return different values if the database has
// been modified by another connection in the interim.
func DataVersion(db *sql.DB) (int64, error) {
	return noSchema.DataVersion(db)
}

// DataVersion provides a way to detect changes to this Schema's
// database file by other processes. Consecutive calls to
// DataVersion return different values if the database has been
// modified by another connection in the interim.
func (s *Schema) DataVersion(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, dataVersion)
}

// EncodingValue represents the text encoding of an SQLite
// database.
type EncodingValue uint

const (
	// EncodingUTF8 is UTF-8 encoding (the default).
	EncodingUTF8 EncodingValue = iota

	// EncodingUTF16 is shorthand for UTF-16 encoding with the
	// native machine byte order. SQLite chooses the correct
	// version automatically.
	EncodingUTF16

	// EncodingUTF16LE is little-endian UTF-16 encoding.
	EncodingUTF16LE

	// EncodingUTF16BE is big-endian UTF-16 encoding.
	EncodingUTF16BE
)

// Scan implements the sql.Scanner interface.
func (v *EncodingValue) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid EncodingValue: %T %v", src, src)
	}

	// No need to handle "UTF-16". SQLite should never return it.
	switch strings.ToUpper(s) {
	case "UTF-8":
		*v = EncodingUTF8
	case "UTF-16LE":
		*v = EncodingUTF16LE
	case "UTF-16BE":
		*v = EncodingUTF16BE
	default:
		return fmt.Errorf("unexpected EncodingValue: %v", s)
	}

	return nil
}

func (v EncodingValue) value() (interface{}, error) {

	var s string

	switch v {
	case EncodingUTF8:
		s = "UTF-8"
	case EncodingUTF16:
		s = "UTF-16"
	case EncodingUTF16LE:
		s = "UTF-16le"
	case EncodingUTF16BE:
		s = "UTF-16be"
	default:
		return nil, fmt.Errorf("unexpected EncodingValue: %v", v)
	}

	// We need to quote these strings otherwise SQLite will
	// treat the dashes as minus signs.
	return quote(s), nil
}

// Encoding returns the text encoding used by the main database.
func Encoding(db *sql.DB) (EncodingValue, error) {
	var dest EncodingValue
	return dest, noSchema.pragma(db, encoding, &dest)
}

// SetEncoding sets the text encoding of the main database. It
// must be called before any tables have been created. SetEncoding
// will fail silently if called on an already created database.
func SetEncoding(db *sql.DB, value EncodingValue) error {
	return noSchema.setPragmaValue(db, encoding, value)
}

// ForeignKeys returns a boolean indicating whether foreign key
// constraints are being enforced. By default, foreign keys are
// disabled (unless changed with a compile-time option).
func ForeignKeys(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, foreignKeys)
}

// SetForeignKeys enables or disables enforcement of foreign
// key constraints.
func SetForeignKeys(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, foreignKeys, on)
}

// ForeignKeysDeferred returns a boolean indicating whether
// foreign keys are being deferred. By default, foreign keys
// are not deferred unless explicitly created as "DEFERRABLE
// INITIALLY DEFERRED".
func ForeignKeysDeferred(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, deferForeignKeys)
}

// SetForeignKeysDeferred enables or disables deferring for all
// foreign keys.
//
// Note that this setting requires foreign keys to be enabled
// and only lasts until the next COMMIT or ROLLBACK.
func SetForeignKeysDeferred(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, deferForeignKeys, on)
}

// FreelistCount returns the number of unused pages in the main
// database file.
func FreelistCount(db *sql.DB) (int64, error) {
	return noSchema.FreelistCount(db)
}

// FreelistCount returns the number of unused pages in this
// Schema's database file .
func (s *Schema) FreelistCount(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, freelistCount)
}

// Fullfsync returns a boolean indicating whether the F_FULLFSYNC
// syncing method is used on systems that support it (currently
// only MAC OS X). Fullfsync is disabled by default.
func Fullfsync(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, fullfsync)
}

// SetFullfsync enables or disables F_FULLFSYNC syncing on
// systems that support it (currently only MAC OS X).
func SetFullfsync(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, fullfsync, on)
}

// FullfsyncCheckpoint returns a boolean indicating whether
// F_FULLFSYNC syncing is used for WAL checkpoints. Fullfsync
// is off by default.
func FullfsyncCheckpoint(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, fullfsyncCheckpoint)
}

// SetFullfsyncCheckpoint enables or disables F_FULLFSYNC syncing
// for WAL checkpoint operations. If Fullfsync has already been
// enabled with SetFullfsync, this function has no effect.
func SetFullfsyncCheckpoint(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, fullfsyncCheckpoint, on)
}

// JournalModeValue describes how SQLite handles the rollback
// journal.
type JournalModeValue uint

// JournalModeMemory and JournalModeOff are the only valid
// values for in-memory databases. Any other values will
// fail silently.
const (
	// JournalModeDelete deletes the rollback journal at the
	// end of each transaction. This is the default setting.
	JournalModeDelete JournalModeValue = iota

	// JournalModeTruncate truncates the rollback journal to
	// zero length instead of deleting it.
	JournalModeTruncate

	// JournalModePersist invalidates the rollback journal by
	// overwriting the journal header with zeroes instead of
	// deleting it.
	JournalModePersist

	// JournalModeMemory holds the rollback journal in RAM
	// instead of on disk.
	JournalModeMemory

	// JournalModeWAL puts the database into WAL mode. Instead
	// of a rollback journal, SQLite maintains a write-ahead
	// log which is periodically saved to the database.
	JournalModeWAL

	// JournalModeOff turns off the rollback journal, disabling
	// transactions and rollbacks altogether.
	JournalModeOff
)

// Scan implements the sql.Scanner interface.
func (v *JournalModeValue) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid JournalModeValue: %T %v", src, src)
	}

	switch strings.ToUpper(s) {
	case "DELETE":
		*v = JournalModeDelete
	case "TRUNCATE":
		*v = JournalModeTruncate
	case "PERSIST":
		*v = JournalModePersist
	case "MEMORY":
		*v = JournalModeMemory
	case "WAL":
		*v = JournalModeWAL
	case "OFF":
		*v = JournalModeOff
	default:
		return fmt.Errorf("unexpected JournalModeValue: %v", s)
	}

	return nil
}

func (v JournalModeValue) value() (interface{}, error) {

	var s string

	switch v {
	case JournalModeDelete:
		s = "DELETE"
	case JournalModeTruncate:
		s = "TRUNCATE"
	case JournalModePersist:
		s = "PERSIST"
	case JournalModeMemory:
		s = "MEMORY"
	case JournalModeWAL:
		s = "WAL"
	case JournalModeOff:
		s = "OFF"
	default:
		return nil, fmt.Errorf("unexpected JournalModeValue %v", v)
	}

	return s, nil
}

// JournalMode returns the status of the rollback journal for
// the main database.
func JournalMode(db *sql.DB) (JournalModeValue, error) {
	return noSchema.JournalMode(db)
}

// JournalMode returns the status of the rollback journal for
// this Schema.
func (s *Schema) JournalMode(db *sql.DB) (JournalModeValue, error) {
	var dest JournalModeValue
	return dest, s.pragma(db, journalMode, &dest)
}

// SetJournalMode sets the status of the rollback journal for
// all attached databases.
func SetJournalMode(db *sql.DB, value JournalModeValue) error {
	return noSchema.SetJournalMode(db, value)
}

// SetJournalMode sets the status of the rollback journal for
// this Schema.
func (s *Schema) SetJournalMode(db *sql.DB, value JournalModeValue) error {
	return s.setPragmaValue(db, journalMode, value)
}

// JournalSizeLimit returns the maximum size in bytes of the
// rollback journal or write-ahead log  after a transaction or
// checkpoint. This is only relevant when journal mode is
// JournalModePersist or JournalModeWAL (see SetJournalMode).
func JournalSizeLimit(db *sql.DB) (int64, error) {
	return noSchema.JournalSizeLimit(db)
}

// JournalSizeLimit returns the maximum number of bytes that
// the rollback journal or write-ahead log will occupy on the
// filesystem after a transaction or checkpoint. This is only
// relevant when journal mode is set to JournalModePersist or
// JournalModeWAL (see SetJournalMode).
func (s *Schema) JournalSizeLimit(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, journalSizeLimit)
}

// SetJournalSizeLimit sets the maximum number of bytes that
// the rollback journal or write-ahead log will occupy on the
// filesystem after a transaction or checkpoint. Negative
// values imply no limit. Pass zero to always truncate rollback
// journals and WAL files to the minimum possible size.
func SetJournalSizeLimit(db *sql.DB, bytes int64) error {
	return noSchema.SetJournalSizeLimit(db, bytes)
}

// SetJournalSizeLimit sets the maximum number of bytes that
// the rollback journal or write-ahead log will occupy on the
// filesystem after a transaction or checkpoint. Negative
// values imply no limit. Pass zero to always truncate rollback
// journals and WAL files to the minimum possible size.
func (s *Schema) SetJournalSizeLimit(db *sql.DB, bytes int64) error {
	return s.setPragmaInt(db, journalSizeLimit, bytes)
}

// LegacyFileFormat returns a boolean indicating the status of
// SQLite's legacy file mode. When legacy file mode is on, new
// databases are created in a format compatible with all SQLite
// 3 versions. When legacy file mode is off (the default), new
// databases are created in the latest format which requires a
// minimum of SQLite version 3.3.0.
//
// Note that LegacyFileFormat says nothing about the format of
// existing SQLite databases. It only indicates what format new
// databases will use.
func LegacyFileFormat(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, legacyFileFormat)
}

// SetLegacyFileFormat enables or disables legacy file mode. When
// legacy file mode is on, new databases are created in a format
// compatible with all SQLite 3 versions. When legacy file mode
// is off (the default), new databases are created in the latest
// format which requires a minimum of SQLite version 3.3.0.
func SetLegacyFileFormat(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, legacyFileFormat, on)
}

// LockingModeValue represents the database locking mode.
type LockingModeValue uint

const (
	// LockingModeNormal unlocks the database file after
	// each transaction. This is the default behaviour.
	LockingModeNormal LockingModeValue = iota

	// LockingModeExclusive maintains database locks until
	// the database connection is closed.
	LockingModeExclusive
)

// Scan implements the sql.Scanner interface.
func (v *LockingModeValue) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid LockingModeValue: %T %v", src, src)
	}

	switch strings.ToUpper(s) {
	case "NORMAL":
		*v = LockingModeNormal
	case "EXCLUSIVE":
		*v = LockingModeExclusive
	default:
		return fmt.Errorf("unexpected LockingModeValue: %v", s)
	}

	return nil
}

func (v LockingModeValue) value() (interface{}, error) {

	var s string

	switch v {
	case LockingModeNormal:
		s = "NORMAL"
	case LockingModeExclusive:
		s = "EXCLUSIVE"
	default:
		return nil, fmt.Errorf("Unexpected LockingModeValue %v", v)
	}

	return s, nil
}

// LockingMode returns the locking mode for the main database.
// The default LockingMode is is LockingModeExclusive for temp
// and in-memory databases, and LockingModeNormal for all other
// databases.
func LockingMode(db *sql.DB) (LockingModeValue, error) {
	return noSchema.LockingMode(db)
}

// LockingMode returns the locking mode for this Schema. The
// default LockingMode is is LockingModeExclusive for temp and
// in-memory databases, and LockingModeNormal for all other
// databases.
func (s *Schema) LockingMode(db *sql.DB) (LockingModeValue, error) {
	var dest LockingModeValue
	return dest, s.pragma(db, lockingMode, &dest)
}

// SetLockingMode sets the locking mode for all attached databases.
// Note that the locking mode of temp and in-memory databases cannot
// be changed.
func SetLockingMode(db *sql.DB, value LockingModeValue) error {
	return noSchema.SetLockingMode(db, value)
}

// SetLockingMode sets the locking mode for this Schema. Note
// that the locking mode of temp and in-memory databases cannot
// be changed.
func (s *Schema) SetLockingMode(db *sql.DB, value LockingModeValue) error {
	return s.setPragmaValue(db, lockingMode, value)
}

// MaxPageCount returns the maximum number of pages that the main
// database file can contain.
func MaxPageCount(db *sql.DB) (int64, error) {
	return noSchema.MaxPageCount(db)
}

// MaxPageCount returns the maximum number of pages that this
// Schema's database file can contain.
func (s *Schema) MaxPageCount(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, maxPageCount)
}

// SetMaxPageCount sets the maximum number of pages that the main
// database file can contain and returns the new value. The maximum
// cannot be reduced below the current database size.
func SetMaxPageCount(db *sql.DB, pages int64) (int64, error) {
	return noSchema.SetMaxPageCount(db, pages)
}

// SetMaxPageCount sets the maximum number of pages that this
// Schema's database file can contain and returns the new value.
// The maximum cannot be reduced below the current database size.
func (s *Schema) SetMaxPageCount(db *sql.DB, pages int64) (int64, error) {
	return 0, s.setPragmaInt(db, maxPageCount, pages)
}

// MemoryMapSize returns the maximum number of bytes that are set
// aside for memory-mapped I/O on the main database.
func MemoryMapSize(db *sql.DB) (int64, error) {
	return noSchema.MemoryMapSize(db)
}

// MemoryMapSize returns the maximum number of bytes that are set
// aside for memory-mapped I/O in this Schema.
func (s *Schema) MemoryMapSize(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, memoryMapSize)
}

// SetMemoryMapSize sets the maximum number of bytes that are set
// aside for memory-mapped I/O in all attached databases. Pass zero
// to disable memory-mapped I/O. Pass a negative number to revert
// to the compile-time default.
func SetMemoryMapSize(db *sql.DB, bytes int64) error {
	return noSchema.SetMemoryMapSize(db, bytes)
}

// SetMemoryMapSize sets the maximum number of bytes that are set
// aside for memory-mapped I/O in this Schema. Pass zero to disable
// memory-mapped I/O. Pass a negative value to revert to the
// compile-time default.
func (s *Schema) SetMemoryMapSize(db *sql.DB, bytes int64) error {
	return s.setPragmaInt(db, memoryMapSize, bytes)
}

// PageCount returns the total number of pages in the main
// database file.
func PageCount(db *sql.DB) (int64, error) {
	return noSchema.PageCount(db)
}

// PageCount returns the total number of pages in this Schema's
// database file.
func (s *Schema) PageCount(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, pageCount)
}

// PageSizeValue represents a valid page size for SQLite.
type PageSizeValue uint

// SQLite page size must be a power of 2 between 512 bytes and
// 64KiB.
const (
	PageSize512 PageSizeValue = 2 << (8 + iota)
	PageSize1024
	PageSize2048
	PageSize4096
	PageSize8192
	PageSize16384
	PageSize32768
	PageSize65536
)

// Scan implements the sql.Scanner interface.
func (v *PageSizeValue) Scan(src interface{}) error {

	i, ok := toInt(src)
	if !ok {
		return fmt.Errorf("Invalid PageSizeValue %T %v", src, src)
	}

	values := []PageSizeValue{
		PageSize512,
		PageSize1024,
		PageSize2048,
		PageSize4096,
		PageSize8192,
		PageSize16384,
		PageSize32768,
		PageSize65536,
	}

	for j := range values {
		if i == 2<<(8+uint(j)) {
			*v = values[j]
			return nil
		}
	}

	return fmt.Errorf("unexpected PageSizeValue %d", i)
}

func (v PageSizeValue) value() (interface{}, error) {
	switch v {
	case PageSize512, PageSize1024, PageSize2048, PageSize4096, PageSize8192, PageSize16384, PageSize32768, PageSize65536:
		return int64(v), nil
	default:
		return nil, fmt.Errorf("unexpected PageSizeValue %v", v)
	}
}

// PageSize returns the page size of the main database. The
// default is 4096 bytes (unless changed by compile-time option).
func PageSize(db *sql.DB) (PageSizeValue, error) {
	return noSchema.PageSize(db)
}

// PageSize returns the page size of this Schema. The default is
// 4096 bytes (unless changed by compile-time option).
func (s *Schema) PageSize(db *sql.DB) (PageSizeValue, error) {
	var dest PageSizeValue
	return dest, s.pragma(db, pageSize, &dest)
}

// SetPageSize sets the page size of the main database. If the
// database already exists (i.e. it already contains tables),
// the new page size will not take effect until the database
// is rebuilt with the VACUUM command.
func SetPageSize(db *sql.DB, value PageSizeValue) error {
	return noSchema.SetPageSize(db, value)
}

// SetPageSize sets the page size of this Schema. If the database
// already exists (i.e. it already contains tables), the new page
// size will not take effect until the database is rebuilt with
// the VACUUM command.
func (s *Schema) SetPageSize(db *sql.DB, value PageSizeValue) error {
	return s.setPragmaValue(db, pageSize, value)
}

// ReadOnly returns a boolean indicating whether the current
// database connection is in read-only mode.
func ReadOnly(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, readOnly)
}

// SetReadOnly enables or disables read-only mode on the current
// database connection. When read-only mode is on, all changes
// to databases via this connection are prevented.
func SetReadOnly(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, readOnly, on)
}

// ReadUncommitted returns a boolean indicating whether shared
// cache database connections are able to read data without
// requiring table locks. Uncommitted reads are disabled by
// default.
func ReadUncommitted(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, readUncommitted)
}

// SetReadUncommitted enables or disables the ability of shared
// cache database connections to read data without requiring
// table locks.
func SetReadUncommitted(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, readUncommitted, on)
}

// RecursiveTriggers returns a boolean indicating whether
// recursive triggers are enabled.
func RecursiveTriggers(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, recursiveTriggers)
}

// SetRecursiveTriggers enables or disables recursive triggers.
func SetRecursiveTriggers(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, recursiveTriggers, on)
}

// ReverseUnorderedSelects returns a boolean indicating whether
// results from queries without an ORDER BY clause appear in
// the default SQLite order, or the reverse.
func ReverseUnorderedSelects(db *sql.DB) (bool, error) {
	return noSchema.pragmaBool(db, reverseUnorderedSelects)
}

// SetReverseUnorderedSelects specifies whether results from
// queries without an ORDER BY clause should be returned in the
// default SQLite order or the reverse. Use this to ensure that
// client code does not rely on the default ordering.
func SetReverseUnorderedSelects(db *sql.DB, on bool) error {
	return noSchema.setPragmaBool(db, reverseUnorderedSelects, on)
}

// SecureDeleteValue represents SQLite's secure delete status.
type SecureDeleteValue uint

const (
	// SecureDeleteOff means that secure delete is disabled.
	// This is the default (unless changed by a compile-time
	// setting).
	SecureDeleteOff SecureDeleteValue = iota

	// SecureDeleteOn enables secure delete. SQLite will
	// overwrite deleted content with zeroes.
	SecureDeleteOn

	// SecureDeleteFast overwrites deleted content with zeroes
	// but only if doing so does not increase the amount of I/O.
	SecureDeleteFast
)

// Scan implements the sql.Scanner interface.
func (v *SecureDeleteValue) Scan(src interface{}) error {

	i, ok := toInt(src)
	if !ok {
		return fmt.Errorf("invalid SecureDeleteValue %T %v", src, src)
	}

	switch i {
	case 0:
		*v = SecureDeleteOff
	case 1:
		*v = SecureDeleteOn
	case 2:
		*v = SecureDeleteFast
	default:
		return fmt.Errorf("unexpected SecureDeleteValue %d", i)
	}

	return nil
}

func (v SecureDeleteValue) value() (interface{}, error) {
	switch v {
	case SecureDeleteOff:
		return false, nil
	case SecureDeleteOn:
		return true, nil
	case SecureDeleteFast:
		return "FAST", nil
	default:
		return nil, fmt.Errorf("unexpected SecureDeleteValue %v", v)
	}
}

// SecureDelete returns a boolean indicating whether secure
// delete mode is enabled on the main database. In secure
// delete mode, SQLite overwrites deleted data with zeroes.
func SecureDelete(db *sql.DB) (SecureDeleteValue, error) {
	return noSchema.SecureDelete(db)
}

// SecureDelete returns a boolean indicating whether secure
// delete mode is enabled on this Schema. In secure delete mode,
// SQLite overwrites deleted data with zeroes.
func (s *Schema) SecureDelete(db *sql.DB) (SecureDeleteValue, error) {
	var dest SecureDeleteValue
	return dest, s.pragma(db, secureDelete, &dest)
}

// SetSecureDelete enables or disables secure delete mode. In
// secure delete mode, SQLite overwrites deleted data with
// zeroes.
func SetSecureDelete(db *sql.DB, value SecureDeleteValue) error {
	return noSchema.SetSecureDelete(db, value)
}

// SetSecureDelete enables or disables secure delete mode on this
// Schema's database. In secure delete mode, SQLite overwrites
// deleted data with zeroes.
func (s *Schema) SetSecureDelete(db *sql.DB, value SecureDeleteValue) error {
	return s.setPragmaValue(db, secureDelete, value)
}

// SoftHeapLimit returns the maximum amount of heap memory that
// SQLite will allocate under ideal circumstances. Zero means
// that there is no limit. The soft heap limit is advisory only.
// SQLite will exceed it if the alternative would be to return
// an out of memory error.
func SoftHeapLimit(db *sql.DB) (int64, error) {
	return noSchema.pragmaInt(db, softHeapLimit)
}

// SetSoftHeapLimit sets an advisory limit on the amount of heap
// memory that SQLite can allocate. SQLite will reduce the size of
// its cache to try to stay under this limit but it will not fail
// if that is not possible. Pass zero to disable the soft heap limit.
func SetSoftHeapLimit(db *sql.DB, value int64) error {
	return noSchema.setPragmaInt(db, softHeapLimit, value)
}

// SynchronousValue represents SQLite synchronization status.
// It determines how SQLite synchronizes database files with the
// underlying operating system.
type SynchronousValue uint

const (
	// SynchronousOff means that SQLite continues without syncing
	// as soon as it has handed data off to the operating system.
	// This is faster than other synchronization modes but there
	// is a risk of database corruption if power is lost before
	// the data is actually written to the disk.
	SynchronousOff SynchronousValue = iota

	// SynchronousNormal means that SQLite waits for data to be
	// safely written to disk before continuing. It does this at
	// the most critical moments but not as often as in FULL mode.
	// There is still a small chance that data could be corrupted
	// if power is lost at the wrong time.
	SynchronousNormal

	// SynchronousFull guarantees that a power failure will not
	// corrupt the database. This is the default setting.
	SynchronousFull

	// SynchronousExtra is like SynchronousFull but with additional
	// syncing of the rollback journal. It offers extra durability
	// in the case where a power failure closely follows a commit.
	SynchronousExtra
)

// Scan implements the sql.Scanner interface.
func (v *SynchronousValue) Scan(src interface{}) error {

	i, ok := toInt(src)
	if !ok {
		return fmt.Errorf("invalid SynchronousValue: %T %v", src, src)
	}

	switch i {
	case 0:
		*v = SynchronousOff
	case 1:
		*v = SynchronousNormal
	case 2:
		*v = SynchronousFull
	case 3:
		*v = SynchronousExtra
	default:
		return fmt.Errorf("unexpected SynchronousValue: %d", i)
	}

	return nil
}

func (v SynchronousValue) value() (interface{}, error) {
	switch v {
	case SynchronousOff, SynchronousNormal, SynchronousFull, SynchronousExtra:
		return int64(v), nil
	default:
		return nil, fmt.Errorf("unexpected SynchronousValue %v", v)
	}
}

// Synchronous returns the synchronization status of the main
// database.
func Synchronous(db *sql.DB) (SynchronousValue, error) {
	return noSchema.Synchronous(db)
}

// Synchronous returns the synchronization status of this Schema.
func (s *Schema) Synchronous(db *sql.DB) (SynchronousValue, error) {
	var dest SynchronousValue
	return dest, s.pragma(db, synchronous, &dest)
}

// SetSynchronous sets the synchronization status of the main
// database.
func SetSynchronous(db *sql.DB, value SynchronousValue) error {
	return noSchema.SetSynchronous(db, value)
}

// SetSynchronous sets the synchronization status of this Schema.
func (s *Schema) SetSynchronous(db *sql.DB, value SynchronousValue) error {
	return s.setPragmaValue(db, synchronous, value)
}

// TempStoreValue represents the preferred location of temporary
// database tables and indexes.
type TempStoreValue uint

const (
	// TempStoreDefault uses the SQLITE_TEMP_STORE compile-time
	// setting to determine where temporary files should be stored.
	TempStoreDefault TempStoreValue = iota

	// TempStoreFile stores temporary files on disk.
	TempStoreFile

	// TempStoreMemory stores temporary files in memory.
	TempStoreMemory
)

// Scan implements the sql.Scanner interface.
func (v *TempStoreValue) Scan(src interface{}) error {

	i, ok := toInt(src)
	if !ok {
		return fmt.Errorf("invalid TempStoreValue: %T %v", src, src)
	}

	switch i {
	case 0:
		*v = TempStoreDefault
	case 1:
		*v = TempStoreFile
	case 2:
		*v = TempStoreMemory
	default:
		return fmt.Errorf("unexpected TempStoreValue: %d", i)
	}

	return nil
}

func (v TempStoreValue) value() (interface{}, error) {
	switch v {
	case TempStoreDefault, TempStoreFile, TempStoreMemory:
		return int64(v), nil
	default:
		return nil, fmt.Errorf("unexpected TempStoreValue %v", v)
	}
}

// TempStore returns the location for temporary tables and
// indexes.
func TempStore(db *sql.DB) (TempStoreValue, error) {
	var dest TempStoreValue
	return dest, noSchema.pragma(db, tempStore, &dest)
}

// SetTempStore sets the location for temporary tables and
// indexes. Changing this location deletes all existing temp
// tables and indexes.
func SetTempStore(db *sql.DB, value TempStoreValue) error {
	return noSchema.setPragmaValue(db, tempStore, value)
}

// Threads returns the maximum number of auxiliary threads that
// SQLite may launch in a query. The default is zero.
func Threads(db *sql.DB) (int64, error) {
	return noSchema.pragmaInt(db, threads)
}

// SetThreads sets the maximum number of auxiliary threads that
// SQLite may launch in a query.
func SetThreads(db *sql.DB, value int64) error {
	return noSchema.setPragmaInt(db, threads, value)
}

// UserVersion returns the user version integer from the main
// database file header. This is an application-specific value,
// not otherwise used by SQLite.
func UserVersion(db *sql.DB) (int64, error) {
	return noSchema.UserVersion(db)
}

// UserVersion returns the user version integer from this Schema's
// database file header. This is an application-specific value,
// not otherwise used by SQLite.
func (s *Schema) UserVersion(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, userVersion)
}

// SetUserVersion sets the user version integer in the main
// database file header. This is an application-specific value,
// not otherwise used by SQLite.
func SetUserVersion(db *sql.DB, value int64) error {
	return noSchema.SetUserVersion(db, value)
}

// SetUserVersion sets the user version integer in this Schema's
// database file header. This is an application-specific value,
// not otherwise used by SQLite.
func (s *Schema) SetUserVersion(db *sql.DB, value int64) error {
	return s.setPragmaInt(db, userVersion, value)
}

// WALAutoCheckpointThreshold returns the threshold number of
// pages for auto-checkpointing the write-ahead log. A checkpoint
// operation runs automatically whenever the write-ahead log equals
// or exceeds the threshold.
func WALAutoCheckpointThreshold(db *sql.DB) (int64, error) {
	return noSchema.WALAutoCheckpointThreshold(db)
}

// WALAutoCheckpointThreshold returns the threshold number of
// pages for auto-checkpointing the write-ahead log. A checkpoint
// operation runs automatically whenever the write-ahead log equals
// or exceeds the threshold.
func (s *Schema) WALAutoCheckpointThreshold(db *sql.DB) (int64, error) {
	return s.pragmaInt(db, walAutoCheckpoint)
}

// SetWALAutoCheckpointThreshold sets the threshold number of
// pages for auto-checkpointing the write-ahead log. A checkpoint
// operation runs automatically whenever the write-ahead log equals
// or exceeds the given number of pages. Pass zero to disable
// auto-checkpointing.
//
// Note that this function does nothing if SQLite is not in WAL mode
// (see SetJournalMode).
func SetWALAutoCheckpointThreshold(db *sql.DB, pages int64) error {
	return noSchema.SetWALAutoCheckpointThreshold(db, pages)
}

// SetWALAutoCheckpointThreshold sets the threshold number of
// pages for auto-checkpointing the write-ahead log. A checkpoint
// operation runs automatically whenever the write-ahead log equals
// or exceeds the given number of pages. Pass zero to disable
// auto-checkpointing.
//
// Note that this function does nothing if SQLite is not in WAL mode
// (see SetJournalMode).
func (s *Schema) SetWALAutoCheckpointThreshold(db *sql.DB, pages int64) error {
	return s.setPragmaInt(db, walAutoCheckpoint, pages)
}

// Lists

// ListCollations returns the collating functions in the current
// database. SQLite provides three built-in collating functions:
// BINARY, NOCASE and RTRIM.
func ListCollations(db *sql.DB) ([]string, error) {

	q := `SELECT name FROM pragma_collation_list ORDER BY seq`

	return queryStrings("pragma collation_list", db, q)
}

// ListCompileOptions returns the options used to build SQLite.
func ListCompileOptions(db *sql.DB) ([]string, error) {

	q := `SELECT compile_options FROM pragma_compile_options ORDER BY compile_options`

	return queryStrings("pragma compile_options", db, q)
}

// A Database represents a schema in the current database
// connection.
type Database struct {

	// Name is the name of the database. The database used to
	// open the connection is always named "main". The temporary
	// database, if present, is named "temp".
	Name string

	// Path is the location of the database file.
	Path string
}

// ListDatabases returns the names and paths of all databases
// attached to the current database connection.
func ListDatabases(db *sql.DB) ([]Database, error) {

	q := `SELECT name, file FROM pragma_database_list ORDER BY seq`

	var dbs []Database

	err := queryRows(&dbs, db, q)
	if err != nil {
		return nil, fmt.Errorf("pragma database_list: %s", err)
	}

	return dbs, nil
}

// A ForeignKeyAction is an action that takes place when modifying
// or deleting parent keys.
type ForeignKeyAction uint

const (
	// ForeignKeyActionNone means that no special action is taken.
	// This is the default.
	ForeignKeyActionNone ForeignKeyAction = iota

	// ForeignKeyActionRestrict prohibits the application from
	// modifying or deleting keys is prohibited.
	ForeignKeyActionRestrict

	// ForeignKeyActionSetNull sets the child key values to NULL
	// when a parent key value is modified or deleted.
	ForeignKeyActionSetNull

	// ForeignKeyActionSetDefault sets the child key values to
	// the default column value when a parent key value is modified
	// or deleted.
	ForeignKeyActionSetDefault

	// ForeignKeyActionCascade propogates parent key changes to
	// the child rows. If a parent key is deleted, the child rows
	// are also deleted. If a parent key value is updated, the
	// child key value is similarly updated.
	ForeignKeyActionCascade
)

// Scan implements the sql.Scanner interface.
func (v *ForeignKeyAction) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid ForeignKeyAction %T %v", src, src)
	}

	switch strings.ToUpper(s) {
	case "NO ACTION":
		*v = ForeignKeyActionNone
	case "RESTRICT":
		*v = ForeignKeyActionRestrict
	case "SET NULL":
		*v = ForeignKeyActionSetNull
	case "SET DEFAULT":
		*v = ForeignKeyActionSetDefault
	case "CASCADE":
		*v = ForeignKeyActionCascade
	default:
		return fmt.Errorf("unexpected ForeignKeyAction %s", s)
	}

	return nil
}

// ForeignKey represents a foreign key constraint.
type ForeignKey struct {
	ID          int64
	ChildTable  string
	ChildKey    []string
	ParentTable string
	ParentKey   []sql.NullString // NULL if a parent key field is not specified.
	OnUpdate    ForeignKeyAction
	OnDelete    ForeignKeyAction
}

// ListForeignKeys returns foreign key information for the given
// table. Pass a blank string to return information for all tables
// in the main database.
func ListForeignKeys(db *sql.DB, tableName string) ([]ForeignKey, error) {
	return noSchema.ListForeignKeys(db, tableName)
}

// ListForeignKeys returns foreign key information for the given
// table. Pass a blank string to return information for all tables
// in this Schema.
func (s *Schema) ListForeignKeys(db *sql.DB, tableName string) ([]ForeignKey, error) {

	if tableName != "" {
		return s.listForeignKeys(db, tableName)
	}

	names, err := s.getTableNames(db)
	if err != nil {
		return nil, err
	}

	var foreignKeys []ForeignKey

	for _, tableName = range names {

		fks, err := s.listForeignKeys(db, tableName)
		if err != nil {
			return nil, err
		}

		foreignKeys = append(foreignKeys, fks...)
	}

	return foreignKeys, nil
}

func (s *Schema) listForeignKeys(db *sql.DB, tableName string) ([]ForeignKey, error) {

	params := []interface{}{tableName}
	if s.name != "" {
		params = append(params, s.name)
	}

	q :=
		`SELECT
            id,
            "table",
            "from",
			"to",
			on_update,
			on_delete
        FROM
			pragma_` + foreignKeyList + `(` + placeholders(params) + `)
        ORDER BY
            id, seq`

	var rows []struct {
		ID       int64
		Table    string
		From     string
		To       sql.NullString
		OnUpdate ForeignKeyAction
		OnDelete ForeignKeyAction
	}

	err := queryRows(&rows, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("PRAGMA %s(%q): %s", s.qualify(foreignKeyList), tableName, err)
	}

	var fk *ForeignKey
	var foreignKeys []ForeignKey

	for _, r := range rows {

		if fk == nil || r.ID != fk.ID {

			foreignKeys = append(foreignKeys, ForeignKey{
				ID:          r.ID,
				ChildTable:  tableName,
				ParentTable: r.Table,
				OnUpdate:    r.OnUpdate,
				OnDelete:    r.OnDelete,
			})

			fk = &foreignKeys[len(foreignKeys)-1]
		}

		fk.ChildKey = append(fk.ChildKey, r.From)
		fk.ParentKey = append(fk.ParentKey, r.To)
	}

	return foreignKeys, nil
}

// IndexType describes how an index was created.
type IndexType uint

const (
	// IndexTypeDefault denotes an index created by the user
	// with a CREATE INDEX statement.
	IndexTypeDefault IndexType = iota

	// IndexTypeUnique denotes an index created by SQLite to
	// enforce a UNIQUE column constraint.
	IndexTypeUnique

	// IndexTypePrimaryKey denotes an index created by SQLite
	// to enforce a PRIMARY KEY clause.
	IndexTypePrimaryKey
)

// Scan implements the sql.Scanner interface.
func (t *IndexType) Scan(src interface{}) error {

	s, ok := toString(src)
	if !ok {
		return fmt.Errorf("invalid IndexType: %T %v", src, src)
	}

	switch strings.ToLower(s) {
	case "c":
		*t = IndexTypeDefault
	case "u":
		*t = IndexTypeUnique
	case "pk":
		*t = IndexTypePrimaryKey
	default:
		return fmt.Errorf("unexpected IndexType: %q", s)
	}

	return nil
}

// Index represents an index on a table.
type Index struct {
	Name        string
	Table       string
	Type        IndexType
	IsUnique    bool
	IsPartial   bool
	ColumnNames []sql.NullString // NULL if the column is an expression (e.g. a+b)
}

// ListIndexes returns information about the indexes associated
// with the given table. Pass a blank string to return index
// information for all tables in the main database.
func ListIndexes(db *sql.DB, tableName string) ([]Index, error) {
	return noSchema.ListIndexes(db, tableName)
}

// ListIndexes returns information about the indexes associated
// with the given table. Pass a blank string to return index
// information for all tables in this Schema.
func (s *Schema) ListIndexes(db *sql.DB, tableName string) ([]Index, error) {

	if tableName != "" {
		return s.listIndexes(db, tableName)
	}

	names, err := s.getTableNames(db)
	if err != nil {
		return nil, err
	}

	var indexes []Index

	for _, tableName = range names {

		idxs, err := s.listIndexes(db, tableName)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, idxs...)
	}

	return indexes, nil
}

func (s *Schema) listIndexes(db *sql.DB, tableName string) ([]Index, error) {

	placeholder := ""
	params := []interface{}{tableName}

	if s.name != "" {
		placeholder = ", ?"
		params = append(params, s.name, s.name)
	}

	q :=
		`SELECT
            t1.name,
            t1.origin,
            t1."unique",
            t1.partial,
            t2.name
        FROM
            pragma_` + indexList + `(?` + placeholder + `) t1
        INNER JOIN
            pragma_` + indexInfo + `(t1.name` + placeholder + `) t2
        ORDER BY
            t1.seq, t2.seqno`

	var rows []struct {
		Name       string
		Type       IndexType
		Unique     bool
		Partial    bool
		ColumnName sql.NullString
	}

	err := queryRows(&rows, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("PRAGMA %s(%q): %s", s.qualify(indexList), tableName, err)
	}

	var idx *Index
	var indexes []Index

	for _, r := range rows {

		if idx == nil || r.Name != idx.Name {

			indexes = append(indexes, Index{
				Name:      r.Name,
				Table:     tableName,
				Type:      r.Type,
				IsUnique:  r.Unique,
				IsPartial: r.Partial,
			})

			idx = &indexes[len(indexes)-1]
		}

		idx.ColumnNames = append(idx.ColumnNames, r.ColumnName)
	}

	return indexes, nil
}

const (
	// TableRankRowID is the TableRank of an IndexColumn that
	// refers to the table's ROWID.
	TableRankRowID = -1

	// TableRankExpression is the TableRank of an IndexColumn
	// that refers to an expression (e.g. a+b).
	TableRankExpression = -2
)

// IndexColumn represents a column in an index.
type IndexColumn struct {
	Name       sql.NullString // NULL if the column is an expression (e.g. a+b)
	Rank       int
	TableRank  int
	Descending bool
	Collation  string
	IsKey      bool
}

// ListIndexColumns returns column information for the given index.
// Only key columns, those explicitly mentioned in the SQL, are
// included.
func ListIndexColumns(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return noSchema.listIndexColumns(db, indexName, true)
}

// ListIndexColumns returns column information for the given index.
// Only key columns, those explicitly mentioned in the SQL, are
// included.
func (s *Schema) ListIndexColumns(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return s.listIndexColumns(db, indexName, true)
}

// ListIndexColumnsAux returns column information for the given
// index. Unlike ListIndexColumns, it includes any additional
// columns that SQLite inserts into the index (these 'auxiliary'
// columns appear after the key columns).
func ListIndexColumnsAux(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return noSchema.listIndexColumns(db, indexName, false)
}

// ListIndexColumnsAux returns column information for the given
// index. Unlike ListIndexColumns, it includes any additional
// columns that SQLite inserts into the index (these 'auxiliary'
// columns appear after the key columns).
func (s *Schema) ListIndexColumnsAux(db *sql.DB, indexName string) ([]IndexColumn, error) {
	return s.listIndexColumns(db, indexName, false)
}

func (s *Schema) listIndexColumns(db *sql.DB, indexName string, keyColumnsOnly bool) ([]IndexColumn, error) {

	params := []interface{}{indexName}
	if s.name != "" {
		params = append(params, s.name)
	}

	whereClause := "1 = 1"
	if keyColumnsOnly {
		whereClause = "key = 1"
	}

	q :=
		`SELECT
            name,
            seqno,
            cid,
            desc,
            coll,
            key
        FROM
            pragma_` + indexInfoAux + `(` + placeholders(params) + `)
		WHERE
			` + whereClause + `
        ORDER BY
            seqno`

	var columns []IndexColumn

	err := queryRows(&columns, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("PRAGMA %s(%q): %s", s.qualify(indexInfoAux), indexName, err)
	}

	return columns, nil
}

// Column represents a column in a table.
type Column struct {
	ID         int64
	Name       string
	Type       string
	NotNull    bool
	Default    []byte
	PrimaryKey int
}

// ListColumns returns column information for the given table.
func ListColumns(db *sql.DB, tableName string) ([]Column, error) {
	return noSchema.ListColumns(db, tableName)
}

// ListColumns returns column information for the given table.
func (s *Schema) ListColumns(db *sql.DB, tableName string) ([]Column, error) {

	params := []interface{}{tableName}
	if s.name != "" {
		params = append(params, s.name)
	}

	q :=
		`SELECT
            cid,
            name,
            type,
            "notnull",
            dflt_value,
            pk
        FROM
            pragma_` + tableInfo + `(` + placeholders(params) + `)
        ORDER BY
            cid`

	var columns []Column

	err := queryRows(&columns, db, q, params...)
	if err != nil {
		return nil, fmt.Errorf("PRAGMA %s(%q): %s", s.qualify(tableInfo), tableName, err)
	}

	return columns, nil
}

// Operations

// A ForeignKeyViolation represents a violation of a foreign
// key constraint.
type ForeignKeyViolation struct {
	ChildTable   string
	ChildRowID   sql.NullInt64 // NULL if ChildTable is WITHOUT ROWID
	ParentTable  string
	ForeignKeyID int64
}

// RunForeignKeyCheck scans the provided table for foreign key
// violations. Pass a blank string to scan all tables in the main
// database.
func RunForeignKeyCheck(db *sql.DB, tableName string) ([]ForeignKeyViolation, error) {
	return noSchema.RunForeignKeyCheck(db, tableName)
}

// RunForeignKeyCheck scans the provided table for foreign key
// violations. Pass a blank string to scan all tables in the
// Schema.
func (s *Schema) RunForeignKeyCheck(db *sql.DB, tableName string) ([]ForeignKeyViolation, error) {

	q := "PRAGMA " + s.qualify(foreignKeyCheck)
	if tableName != "" {
		q += fmt.Sprintf("(%s)", tableName)
	}

	var violations []ForeignKeyViolation

	err := queryRows(&violations, db, q)
	if err != nil {
		return nil, fmt.Errorf("%s: %s", q, err)
	}

	return violations, nil
}

// RunIncrementalVacuum reduces the main database's freelist
// by the given number of pages. If passed zero or a negative
// number of pages, RunIncrementalVacuum clears the freelist
// entirely.
//
// Note that this function does nothing unless auto vacuum is
// set to incremental mode (see SetAutoVacuum).
func RunIncrementalVacuum(db *sql.DB, pages int) error {
	return noSchema.RunIncrementalVacuum(db, pages)
}

// RunIncrementalVacuum reduces this Schema's freelist by the
// given number of pages. If passed zero or a negative number
// of pages, RunIncrementalVacuum clears the freelist entirely.
//
// Note that this function does nothing unless auto vacuum is
// set to incremental mode (see SetAutoVacuum).
func (s *Schema) RunIncrementalVacuum(db *sql.DB, pages int) error {

	if pages < 0 {
		pages = 0
	}
	q := fmt.Sprintf("PRAGMA %s(%d)", s.qualify(incrementalVacuum), pages)

	_, err := db.Exec(q)
	if err != nil {
		return fmt.Errorf("%s returned error %s", q, err)
	}

	return nil
}

// RunIntegrityCheck runs a diagnostic check on the main database
// and returns details of any data integrity issues (up to the
// given maxmimum number of issues). Issues covered by this check
// include malformed records, missing index entries, and broken
// UNIQUE, CHECK and NOT NULL constraints. Foreign key violations
// are not included (use RunForeignKeyCheck for those).
func RunIntegrityCheck(db *sql.DB, maxErrors int) ([]string, error) {
	return noSchema.RunIntegrityCheck(db, maxErrors)
}

// RunIntegrityCheck runs a diagnostic check on this Schema and
// returns details of any data integrity issues (up to the given
// maxmimum number of issues). Issues covered by this check include
// malformed records, missing index entries, and broken UNIQUE,
// CHECK and NOT NULL constraints. Foreign key violations are not
// included (use RunForeignKeyCheck for those).
func (s *Schema) RunIntegrityCheck(db *sql.DB, maxErrors int) ([]string, error) {
	return s.runIntegrityCheck(db, false, maxErrors)
}

// RunQuickCheck is a faster version of RunIntegrityCheck that
// doesn't verify UNIQUE constraints or index consistency.
func RunQuickCheck(db *sql.DB, maxErrors int) ([]string, error) {
	return noSchema.RunQuickCheck(db, maxErrors)
}

// RunQuickCheck is a faster version of RunIntegrityCheck that
// doesn't verify UNIQUE constraints or index consistency.
func (s *Schema) RunQuickCheck(db *sql.DB, maxErrors int) ([]string, error) {
	return s.runIntegrityCheck(db, true, maxErrors)
}

func (s *Schema) runIntegrityCheck(db *sql.DB, quick bool, maxErrors int) ([]string, error) {

	pragmaName := integrityCheck
	if quick {
		pragmaName = quickCheck
	}

	q := fmt.Sprintf("PRAGMA %s(%d)", s.qualify(pragmaName), maxErrors)

	results, err := queryStrings(q, db, q)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("%s: no results returned", q)
	}

	if len(results) == 1 && strings.ToLower(results[0]) == "ok" {
		results = results[:0]
	}

	return results, nil
}

// RunShrinkMemory attempts to free as much heap memory as
// possible from the current database connection (e.g. by
// discarding any database pages cached in memory).
func RunShrinkMemory(db *sql.DB) error {

	q := "PRAGMA " + shrinkMemory

	_, err := db.Exec(q)
	if err != nil {
		return fmt.Errorf("%s: %s", q, err)
	}

	return nil
}

type WALCheckpointMode uint

const (
	WALCheckpointPassive WALCheckpointMode = iota
	WALCheckpointFull
	WALCheckpointRestart
	WALCheckpointTruncate
)

var (
	ErrWALCheckpointFailed  = errors.New("WAL Checkpoint failed")
	ErrWALCheckpointBlocked = errors.New("WAL Checkpoint blocked")
)

// RunWALCheckpoint transfers transactions from the write-ahead
// log into the database.
//
// This function does nothing if journalling is not set to WAL mode
// (see SetJournalMode).
func RunWALCheckpoint(db *sql.DB, mode WALCheckpointMode) (int64, int64, error) {
	return noSchema.RunWALCheckpoint(db, mode)
}

// RunWALCheckpoint transfers transactions from the write-ahead
// log into the database.
//
// This function does nothing if journalling is not set to WAL mode
// (see SetJournalMode).
func (s *Schema) RunWALCheckpoint(db *sql.DB, mode WALCheckpointMode) (int64, int64, error) {

	var arg string

	switch mode {
	case WALCheckpointPassive:
		arg = "PASSIVE"
	case WALCheckpointFull:
		arg = "FULL"
	case WALCheckpointRestart:
		arg = "RESTART"
	case WALCheckpointTruncate:
		arg = "TRUNCATE"
	default:
		return -1, -1, fmt.Errorf("unexpected checkpoint mode %v", mode)
	}

	var blocked bool
	var total, checked int64

	q := fmt.Sprintf("PRAGMA %s(%s)", s.qualify(walCheckpoint), arg)

	err := db.QueryRow(q).Scan(&blocked, &total, &checked)
	if err != nil {
		return -1, -1, fmt.Errorf("%s: %s", q, err)
	}

	switch {
	case blocked:
		err = ErrWALCheckpointBlocked
	case total == -1 && checked == -1:
		err = ErrWALCheckpointFailed
	}

	return total, checked, err
}

// Schema helpers

func (s *Schema) pragmaBool(db *sql.DB, name string) (bool, error) {
	var dest bool
	return dest, s.pragma(db, name, &dest)
}

func (s *Schema) pragmaInt(db *sql.DB, name string) (int64, error) {
	var dest int64
	return dest, s.pragma(db, name, &dest)
}

func (s *Schema) pragma(db *sql.DB, name string, dest interface{}) error {

	q := "PRAGMA " + s.qualify(name)

	if err := db.QueryRow(q).Scan(dest); err != nil {
		return fmt.Errorf("%s returned error %s", q, err)
	}

	return nil
}

func (s *Schema) setPragmaBool(db *sql.DB, name string, value bool) error {
	return s.setPragma(db, name, value)
}

func (s *Schema) setPragmaInt(db *sql.DB, name string, value int64) error {
	return s.setPragma(db, name, value)
}

type valuer interface {
	value() (interface{}, error)
}

func (s *Schema) setPragmaValue(db *sql.DB, name string, v valuer) error {
	value, err := v.value()
	if err != nil {
		return err
	}
	return s.setPragma(db, name, value)
}

func (s *Schema) setPragma(db *sql.DB, name string, value interface{}) error {

	q := fmt.Sprintf("PRAGMA %s = %v", s.qualify(name), value)

	if _, err := db.Exec(q); err != nil {
		return fmt.Errorf("%s returned error %s", q, err)
	}

	return nil
}

func (s *Schema) getTableNames(db *sql.DB) ([]string, error) {
	return s.getMasterTableNames(db, "table")
}

func (s *Schema) getMasterTableNames(db *sql.DB, typ string) ([]string, error) {

	q := fmt.Sprintf("SELECT name FROM %s WHERE type = ? ORDER BY name", s.qualify("sqlite_master"))
	return queryStrings("sqlite_master", db, q, typ)
}

func (s *Schema) qualify(suffix string) string {
	if s.name == "" {
		return suffix
	}
	return fmt.Sprintf("%s.%s", s.name, suffix)
}

// Helpers

func queryStrings(prefix string, db *sql.DB, q string, args ...interface{}) ([]string, error) {

	var values []string

	err := queryRows(&values, db, q, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %s", prefix, err)
	}

	return values, nil
}

func toInt(v interface{}) (int64, bool) {
	switch i := v.(type) {
	case int64:
		return i, true
	default:
		return 0, false
	}
}

func toString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

func placeholders(vals []interface{}) string {
	return strings.Join(strings.Split(strings.Repeat("?", len(vals)), ""), ", ")
}

func quote(s string) string {
	return `'` + s + `'`
}

func abs(i int64) int64 {
	if i >= 0 {
		return i
	}
	return i * -1
}
