package pragma_test

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/deepilla/pragma"

	// TODO: Test with other SQLite drivers.
	"github.com/mattn/go-sqlite3"
)

var testFlags struct {
	PropN      int
	InMemory   bool
	MaxThreads int
}

var newDB func() (*sql.DB, func(), error)

func TestMain(m *testing.M) {

	rand.Seed(time.Now().UnixNano())

	flag.IntVar(&testFlags.PropN, "pragma.props", 25, "Number of times to run property tests")
	flag.BoolVar(&testFlags.InMemory, "pragma.memory", false, "Run tests on an in-memory database")
	flag.IntVar(&testFlags.MaxThreads, "pragma.threads", 1, "Max threads to use in Threads property test")
	flag.Parse()

	if testFlags.PropN <= 0 {
		fmt.Fprintln(os.Stderr, "invalid test flag: pragma.props must be greater than 0")
		os.Exit(2)
	}

	if testFlags.MaxThreads <= 0 {
		fmt.Fprintln(os.Stderr, "invalid test flag: pragma.threads must be greater than 0")
		os.Exit(2)
	}

	os.Exit(m.Run())
}

// Test Integer properties

type intGetter func(*sql.DB) (int64, error)
type intSetter func(*sql.DB, int64) error

func _testPropertyInt(t *testing.T, db *sql.DB, max int64, name string, get intGetter, set intSetter) {

	for i := 0; i < testFlags.PropN; i++ {

		value := rand.Int63n(max)

		err := set(db, value)
		if err != nil {
			t.Fatalf("Set %s = %d returned error: %s", name, value, err)
		}

		got, err := get(db)
		if err != nil {
			t.Fatalf("Get %s returned error: %s", name, err)
		}

		if got != value {
			t.Errorf("Expected %s to be %d, got %d", name, value, got)
		}
	}
}

func testPropertyInt(t *testing.T, max int64, name string, get intGetter, set intSetter) {
	testWithDB(t, func(t *testing.T, db *sql.DB) {
		_testPropertyInt(t, db, max, name, get, set)
	})
}

func TestPropertyApplicationID(t *testing.T) {
	testPropertyInt(t, 1598903374, "ApplicationID", pragma.ApplicationID, pragma.SetApplicationID)
}

func TestPropertyBusyTimeout(t *testing.T) {
	testPropertyInt(t, 60000, "BusyTimeout", pragma.BusyTimeout, pragma.SetBusyTimeout)
}

func TestPropertyCacheSizePages(t *testing.T) {

	getter := func(db *sql.DB) (int64, error) {
		n, unit, err := pragma.CacheSize(db)
		if unit != pragma.CacheSizePages {
			return 0, fmt.Errorf("expected unit CacheSizePages, got %v", unit)
		}
		return n, err
	}

	setter := func(db *sql.DB, value int64) error {
		return pragma.SetCacheSize(db, value, pragma.CacheSizePages)
	}

	testPropertyInt(t, 1073741823, "CacheSize", getter, setter)
}

func TestPropertyCacheSizeKiB(t *testing.T) {

	getter := func(db *sql.DB) (int64, error) {
		n, unit, err := pragma.CacheSize(db)
		if n != 0 && unit != pragma.CacheSizeKiB {
			err = fmt.Errorf("expected unit CacheSizeKiB, got %v", unit)
		}
		return n, err
	}

	setter := func(db *sql.DB, value int64) error {
		return pragma.SetCacheSize(db, value, pragma.CacheSizeKiB)
	}

	testPropertyInt(t, 2000, "CacheSize", getter, setter)
}

func TestPropertyJournalSizeLimit(t *testing.T) {
	testPropertyInt(t, 65536, "JournalSizeLimit", pragma.JournalSizeLimit, pragma.SetJournalSizeLimit)
}

func TestPropertyMaxPageCount(t *testing.T) {

	setter := func(db *sql.DB, value int64) error {
		_, err := pragma.SetMaxPageCount(db, value)
		return err
	}

	testPropertyInt(t, 65536, "MaxPageCount", pragma.MaxPageCount, setter)
}

func TestPropertyMemoryMapSize(t *testing.T) {

	if testFlags.InMemory {
		// Skip test for in-memory databases. MemoryMapSize
		// always returns an ErrNoRows error.
		t.SkipNow()
	}

	testPropertyInt(t, 2147418112, "MemoryMapSize", pragma.MemoryMapSize, pragma.SetMemoryMapSize)
}

func TestPropertySoftHeapLimit(t *testing.T) {
	testPropertyInt(t, 65536, "SoftHeapLimit", pragma.SoftHeapLimit, pragma.SetSoftHeapLimit)
}

func TestPropertyThreads(t *testing.T) {
	testPropertyInt(t, int64(testFlags.MaxThreads), "Threads", pragma.Threads, pragma.SetThreads)
}

func TestPropertyUserVersion(t *testing.T) {
	testPropertyInt(t, 1598903374, "UserVersion", pragma.UserVersion, pragma.SetUserVersion)
}

func TestPropertyWALAutoCheckpoint(t *testing.T) {
	testPropertyInt(t, 2000, "WALAutoCheckpoint", pragma.WALAutoCheckpointThreshold, pragma.SetWALAutoCheckpointThreshold)
}

// Test Boolean properties

type boolGetter func(*sql.DB) (bool, error)
type boolSetter func(*sql.DB, bool) error

func _testPropertyBool(t *testing.T, db *sql.DB, name string, get boolGetter, set boolSetter) {

	on, err := get(db)
	if err != nil {
		t.Fatalf("%s returned error: %s", name, err)
	}

	for i := 0; i < testFlags.PropN; i++ {

		on = !on

		err := set(db, on)
		if err != nil {
			t.Fatalf("Set %s = %v returned error: %s", name, on, err)
		}

		got, err := get(db)
		if err != nil {
			t.Fatalf("%s returned error: %s", name, err)
		}

		if got != on {
			t.Errorf("Expected %s to be %v, got %v", name, on, got)
		}
	}
}

func testPropertyBool(t *testing.T, name string, get boolGetter, set boolSetter) {
	testWithDB(t, func(t *testing.T, db *sql.DB) {
		_testPropertyBool(t, db, name, get, set)
	})
}

func TestPropertyAutoIndex(t *testing.T) {
	testPropertyBool(t, "AutoIndex", pragma.AutoIndex, pragma.SetAutoIndex)
}

func TestPropertyCellSizeChecks(t *testing.T) {
	testPropertyBool(t, "CellSizeChecks", pragma.CellSizeChecks, pragma.SetCellSizeChecks)
}

func TestPropertyCheckConstraints(t *testing.T) {
	testPropertyBool(t, "CheckConstraints", pragma.CheckConstraints, pragma.SetCheckConstraints)
}

func TestPropertyForeignKeys(t *testing.T) {
	testPropertyBool(t, "ForeignKeys", pragma.ForeignKeys, pragma.SetForeignKeys)
}

func TestPropertyForeignKeysDeferred(t *testing.T) {
	testPropertyBool(t, "ForeignKeysDeferred", pragma.ForeignKeysDeferred, pragma.SetForeignKeysDeferred)
}

func TestPropertyFullfsync(t *testing.T) {
	testPropertyBool(t, "Fullfsync", pragma.Fullfsync, pragma.SetFullfsync)
}

func TestPropertyFullfsyncCheckpoint(t *testing.T) {
	testPropertyBool(t, "FullfsyncCheckpoint", pragma.FullfsyncCheckpoint, pragma.SetFullfsyncCheckpoint)
}

func TestPropertyLegacyFileFormat(t *testing.T) {
	testPropertyBool(t, "LegacyFileFormat", pragma.LegacyFileFormat, pragma.SetLegacyFileFormat)
}

func TestPropertyReadOnly(t *testing.T) {
	testPropertyBool(t, "ReadOnly", pragma.ReadOnly, pragma.SetReadOnly)
}

func TestPropertyReadUncommitted(t *testing.T) {
	testPropertyBool(t, "ReadUncommitted", pragma.ReadUncommitted, pragma.SetReadUncommitted)
}

func TestPropertyRecursiveTriggers(t *testing.T) {
	testPropertyBool(t, "RecursiveTriggers", pragma.RecursiveTriggers, pragma.SetRecursiveTriggers)
}

func TestPropertyReverseUnorderedSelects(t *testing.T) {
	testPropertyBool(t, "ReverseUnorderedSelects", pragma.ReverseUnorderedSelects, pragma.SetReverseUnorderedSelects)
}

// Test Enum properties

func _testPropertyEnum(t *testing.T, db *sql.DB, name string, values map[string]interface{}, get interface{}, set interface{}) {

	vdb := reflect.ValueOf(db)
	vget := reflect.ValueOf(get)
	vset := reflect.ValueOf(set)

	slicify := func(vals ...reflect.Value) []reflect.Value {
		return vals
	}

	for i := 0; i < testFlags.PropN; i++ {
		for k, v := range values {

			results := vset.Call(slicify(vdb, reflect.ValueOf(v)))

			if len(results) != 1 {
				t.Fatalf("Expected Set %s to return 1 value, got %d", name, len(results))
			}

			if !results[0].IsNil() {
				t.Fatalf("Set %s = %s returned %v", name, k, results[0].Interface())
			}

			results = vget.Call(slicify(vdb))

			if len(results) != 2 {
				t.Fatalf("Expected %s to return 2 values, got %d", name, len(results))
			}

			if !results[1].IsNil() {
				t.Fatalf("%s returned %v", name, results[1].Interface())
			}

			if !reflect.DeepEqual(results[0].Interface(), v) {
				t.Errorf("Expected %s to be %v (%s), got %v", name, v, k, results[0].Interface())
			}
		}
	}
}

func testPropertyEnum(t *testing.T, name string, values map[string]interface{}, get interface{}, set interface{}) {
	testWithDB(t, func(t *testing.T, db *sql.DB) {
		_testPropertyEnum(t, db, name, values, get, set)
	})
}

func TestPropertyAutoVacuum(t *testing.T) {

	values := map[string]interface{}{
		"NONE":        pragma.AutoVacuumNone,
		"FULL":        pragma.AutoVacuumFull,
		"INCREMENTAL": pragma.AutoVacuumIncremental,
	}

	setter := func(db *sql.DB, value pragma.AutoVacuumValue) error {

		if err := pragma.SetAutoVacuum(db, value); err != nil {
			return err
		}

		// We need to run VACUUM after SetAutoVacuum to guarantee
		// changes take effect.
		_, err := db.Exec("VACUUM")
		return err
	}

	testPropertyEnum(t, "AutoVacuum", values, pragma.AutoVacuum, setter)

}
func TestPropertyPageSize(t *testing.T) {

	values := map[string]interface{}{
		"512B": pragma.PageSize512,
		"1K":   pragma.PageSize1024,
		"2K":   pragma.PageSize2048,
		"4K":   pragma.PageSize4096,
		"8K":   pragma.PageSize8192,
		"16K":  pragma.PageSize16384,
		"32K":  pragma.PageSize32768,
		"64K":  pragma.PageSize65536,
	}

	testPropertyEnum(t, "PageSize", values, pragma.PageSize, pragma.SetPageSize)
}

func TestPropertyEncoding(t *testing.T) {

	utf16 := pragma.EncodingUTF16LE
	if !isLittleEndian() {
		utf16 = pragma.EncodingUTF16BE
	}

	values := map[string]interface{}{
		"UTF8":    pragma.EncodingUTF8,
		"UTF16":   utf16,
		"UTF16le": pragma.EncodingUTF16LE,
		"UTF16be": pragma.EncodingUTF16BE,
	}

	testPropertyEnum(t, "Encoding", values, pragma.Encoding, pragma.SetEncoding)
}

func TestPropertyJournalMode(t *testing.T) {

	values := map[string]interface{}{
		"MEMORY": pragma.JournalModeMemory,
		"OFF":    pragma.JournalModeOff,
	}

	if !testFlags.InMemory {
		// The following JournalMode values are invalid for
		// in-memory databases.
		values["DELETE"] = pragma.JournalModeDelete
		values["TRUNCATE"] = pragma.JournalModeTruncate
		values["PERSIST"] = pragma.JournalModePersist
		values["WAL"] = pragma.JournalModeWAL
	}

	testPropertyEnum(t, "JournalMode", values, pragma.JournalMode, pragma.SetJournalMode)
}

func TestPropertyLockingMode(t *testing.T) {

	values := map[string]interface{}{
		"NORMAL":    pragma.LockingModeNormal,
		"EXCLUSIVE": pragma.LockingModeExclusive,
	}

	testPropertyEnum(t, "LockingMode", values, pragma.LockingMode, pragma.SetLockingMode)
}

func TestPropertySecureDelete(t *testing.T) {

	// TODO: Why doesn't setting SecureDelete to FAST work?

	values := map[string]interface{}{
		"OFF": pragma.SecureDeleteOff,
		"ON":  pragma.SecureDeleteOn,
		//"FAST": pragma.SecureDeleteFast,
	}

	testPropertyEnum(t, "SecureDelete", values, pragma.SecureDelete, pragma.SetSecureDelete)
}

func TestPropertySynchronous(t *testing.T) {

	values := map[string]interface{}{
		"OFF":    pragma.SynchronousOff,
		"NORMAL": pragma.SynchronousNormal,
		"FULL":   pragma.SynchronousFull,
		"EXTRA":  pragma.SynchronousExtra,
	}

	testPropertyEnum(t, "Synchronous", values, pragma.Synchronous, pragma.SetSynchronous)
}

func TestPropertyTempStore(t *testing.T) {

	values := map[string]interface{}{
		"DEFAULT": pragma.TempStoreDefault,
		"FILE":    pragma.TempStoreFile,
		"MEMORY":  pragma.TempStoreMemory,
	}

	testPropertyEnum(t, "TempStore", values, pragma.TempStore, pragma.SetTempStore)
}

// Test List functions

func TestListCollations(t *testing.T) {
	testWithDB(t, testListCollations)
}

func testListCollations(t *testing.T, db *sql.DB) {

	colls, err := pragma.ListCollations(db)
	if err != nil {
		t.Fatalf("ListCollations returned error %s", err)
	}

	m := map[string]bool{}
	for _, s := range colls {
		m[s] = true
	}

	// Test for sqlite's built-in collating functions.
	for _, s := range []string{
		"BINARY",
		"NOCASE",
		"RTRIM",
	} {
		if !m[s] {
			t.Errorf("Collating function %s not found", s)
		}
	}
}

func TestListForeignKeys(t *testing.T) {
	testWithDB(t, testListForeignKeys)
}

func testListForeignKeys(t *testing.T, db *sql.DB) {

	data := []struct {
		Name        string
		TableName   string
		SQL         []string
		ForeignKeys []pragma.ForeignKey
	}{
		{
			Name:      "No Foreign Keys",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
			},
		},
		{
			Name:      "Short Format, No Parent Key",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x REFERENCES parent
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey:   make([]sql.NullString, 1),
				},
			},
		},
		{
			Name:      "Short Format",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x REFERENCES parent(px)
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Name:      "Long Format",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Name:      "Long Format, Multiple Fields, No Parent Key",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					FOREIGN KEY(x,y) REFERENCES parent
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x", "y"},
					ParentTable: "parent",
					ParentKey:   make([]sql.NullString, 2),
				},
			},
		},
		{
			Name:      "Long Format, Multiple Fields",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					FOREIGN KEY(x,y) REFERENCES parent(px,py)
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x", "y"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
						nullString("py"),
					},
				},
			},
		},
		{
			// Potentially fragile test:
			// Is the order of multiple foreign keys stable?
			Name:      "Multiple Foreign Keys",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					FOREIGN KEY(x) REFERENCES parent1(px),
					FOREIGN KEY(y) REFERENCES parent2(py)
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"y"},
					ParentTable: "parent2",
					ParentKey: []sql.NullString{
						nullString("py"),
					},
				},
				{
					ID:          1,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent1",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Name: "Multiple Tables",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`DROP TABLE IF EXISTS b`,
				`CREATE TABLE a (
					x REFERENCES parent
				)`,
				`CREATE TABLE b (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey:   make([]sql.NullString, 1),
				},
				{
					ID:          0,
					ChildTable:  "b",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
		{
			Name:      "Set NULL, Set Default",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
						ON DELETE SET NULL
						ON UPDATE SET DEFAULT
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
					OnDelete: pragma.ForeignKeyActionSetNull,
					OnUpdate: pragma.ForeignKeyActionSetDefault,
				},
			},
		},
		{
			Name:      "Cascade, Restrict",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
						ON DELETE CASCADE
						ON UPDATE RESTRICT
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
					OnDelete: pragma.ForeignKeyActionCascade,
					OnUpdate: pragma.ForeignKeyActionRestrict,
				},
			},
		},
		{
			Name:      "No Action",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					FOREIGN KEY(x) REFERENCES parent(px)
						ON DELETE NO ACTION
				)`,
			},
			ForeignKeys: []pragma.ForeignKey{
				{
					ID:          0,
					ChildTable:  "a",
					ChildKey:    []string{"x"},
					ParentTable: "parent",
					ParentKey: []sql.NullString{
						nullString("px"),
					},
				},
			},
		},
	}

	for _, test := range data {

		execute(t, db, test.SQL)

		got, err := pragma.ListForeignKeys(db, test.TableName)
		if err != nil {
			t.Fatalf("ListForeignKeys(%q) returned error %s", test.TableName, err)
		}

		compareStructSlices(t, test.Name, "foreign key", "foreign key(s)", test.ForeignKeys, got)
	}
}

func TestListIndexes(t *testing.T) {
	testWithDB(t, testListIndexes)
}

func testListIndexes(t *testing.T, db *sql.DB) {

	data := []struct {
		Name      string
		TableName string
		SQL       []string
		Indexes   []pragma.Index
	}{
		{
			Name:      "No Table",
			TableName: "xxxxxxxxxx",
		},
		{
			Name:      "No Indexes",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
			},
		},
		{
			Name:      "No Indexes (ROWID alias)",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x INTEGER PRIMARY KEY
				)`,
			},
		},
		{
			Name:      "Basic Index",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE INDEX idx1 ON a(x)`,
			},
			Indexes: []pragma.Index{
				{
					Name:  "idx1",
					Table: "a",
					Type:  pragma.IndexTypeDefault,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Unique Index",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE UNIQUE INDEX idx1 ON a(x)`,
			},
			Indexes: []pragma.Index{
				{
					Name:     "idx1",
					Table:    "a",
					Type:     pragma.IndexTypeDefault,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Partial Index",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE INDEX idx1 ON a(x) WHERE x IS NOT NULL`,
			},
			Indexes: []pragma.Index{
				{
					Name:      "idx1",
					Table:     "a",
					Type:      pragma.IndexTypeDefault,
					IsPartial: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Partial & Unique Index",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x
				)`,
				`CREATE UNIQUE INDEX idx1 ON a(x) WHERE x IS NOT NULL`,
			},
			Indexes: []pragma.Index{
				{
					Name:      "idx1",
					Table:     "a",
					Type:      pragma.IndexTypeDefault,
					IsUnique:  true,
					IsPartial: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Unique Constraint",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x UNIQUE
				)`,
			},
			Indexes: []pragma.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Table:    "a",
					Type:     pragma.IndexTypeUnique,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Primary Key (inline)",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x PRIMARY KEY
				)`,
			},
			Indexes: []pragma.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Table:    "a",
					Type:     pragma.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Primary Key (separate clause)",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					PRIMARY KEY(x)
				)`,
			},
			Indexes: []pragma.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Table:    "a",
					Type:     pragma.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name:      "Multiple Columns",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					z
				)`,
				`CREATE UNIQUE INDEX idx1 ON a(x,y,z)`,
			},
			Indexes: []pragma.Index{
				{
					Name:     "idx1",
					Table:    "a",
					Type:     pragma.IndexTypeDefault,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
						nullString("y"),
						nullString("z"),
					},
				},
			},
		},
		{
			// Potentially fragile test.
			// Is the order of multiple indexes stable?
			Name:      "Multiple Indexes",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x PRIMARY KEY,
					y UNIQUE,
					z
				)`,
				`CREATE INDEX idx1 ON a(z) WHERE z IS NOT NULL`,
			},
			Indexes: []pragma.Index{
				{
					Name:      "idx1",
					Table:     "a",
					Type:      pragma.IndexTypeDefault,
					IsPartial: true,
					ColumnNames: []sql.NullString{
						nullString("z"),
					},
				},
				{
					Name:     "sqlite_autoindex_a_2",
					Table:    "a",
					Type:     pragma.IndexTypeUnique,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("y"),
					},
				},
				{
					Name:     "sqlite_autoindex_a_1",
					Table:    "a",
					Type:     pragma.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
			},
		},
		{
			Name: "Multiple Tables",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`DROP TABLE IF EXISTS b`,
				`CREATE TABLE a (
					x PRIMARY KEY
				)`,
				`CREATE TABLE b (
					x,
					y
				)`,
				`CREATE UNIQUE INDEX idx1 ON b(x,y)`,
			},
			Indexes: []pragma.Index{
				{
					Name:     "sqlite_autoindex_a_1",
					Table:    "a",
					Type:     pragma.IndexTypePrimaryKey,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
					},
				},
				{
					Name:     "idx1",
					Table:    "b",
					Type:     pragma.IndexTypeDefault,
					IsUnique: true,
					ColumnNames: []sql.NullString{
						nullString("x"),
						nullString("y"),
					},
				},
			},
		},
		{
			Name:      "Expression",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`DROP TABLE IF EXISTS b`, // Clear index from previous test
				`CREATE TABLE a (
					x,
					y
				)`,
				`CREATE INDEX idx1 ON a(x+y)`,
			},
			Indexes: []pragma.Index{
				{
					Name:        "idx1",
					Table:       "a",
					Type:        pragma.IndexTypeDefault,
					ColumnNames: make([]sql.NullString, 1),
				},
			},
		},
		{
			Name:      "Mixed Columns/Expressions",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					w,
					x,
					y,
					z
				)`,
				`CREATE INDEX idx1 ON a(w,x+y,z)`,
			},
			Indexes: []pragma.Index{
				{
					Name:  "idx1",
					Table: "a",
					Type:  pragma.IndexTypeDefault,
					ColumnNames: []sql.NullString{
						nullString("w"),
						{},
						nullString("z"),
					},
				},
			},
		},
		{
			Name:      "Multiple Expressions",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					w,
					x,
					y,
					z
				)`,
				`CREATE INDEX idx1 ON a(w*x,y+z)`,
			},
			Indexes: []pragma.Index{
				{
					Name:        "idx1",
					Table:       "a",
					Type:        pragma.IndexTypeDefault,
					ColumnNames: make([]sql.NullString, 2),
				},
			},
		},
	}

	for _, test := range data {

		execute(t, db, test.SQL)

		got, err := pragma.ListIndexes(db, test.TableName)
		if err != nil {
			t.Fatalf("ListIndexes(%q) returned error %s", test.TableName, err)
		}

		compareStructSlices(t, test.Name, "index", "index(es)", test.Indexes, got)
	}
}

func TestListIndexColumns(t *testing.T) {
	testWithDB(t, testListIndexColumns)
}

func testListIndexColumns(t *testing.T, db *sql.DB) {

	data := []struct {
		Name    string
		SQL     []string
		Columns map[string][]pragma.IndexColumn
	}{
		{
			Name: "Basic Index",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					v,
					w,
					x,
					y,
					z
				)`,
				`CREATE INDEX idx1 ON a(v)`,
				`CREATE INDEX idx2 ON a(w,z)`,
				`CREATE INDEX idx3 ON a(v+w,x,y+z)`,
			},
			Columns: map[string][]pragma.IndexColumn{
				"xxxxxx": nil,
				"idx1": {
					{
						Name:      nullString("v"),
						Rank:      0,
						TableRank: 0,
						Collation: "BINARY",
						IsKey:     true,
					},
				},
				"idx2": {
					{
						Name:      nullString("w"),
						Rank:      0,
						TableRank: 1,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Name:      nullString("z"),
						Rank:      1,
						TableRank: 4,
						Collation: "BINARY",
						IsKey:     true,
					},
				},
				"idx3": {
					{
						Rank:      0,
						TableRank: pragma.TableRankExpression,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Name:      nullString("x"),
						Rank:      1,
						TableRank: 2,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Rank:      2,
						TableRank: pragma.TableRankExpression,
						Collation: "BINARY",
						IsKey:     true,
					},
				},
			},
		},
	}

	for _, test := range data {

		execute(t, db, test.SQL)

		for indexName, columns := range test.Columns {

			got, err := pragma.ListIndexColumns(db, indexName)
			if err != nil {
				t.Fatalf("ListIndexColumns(%q) returned error %s", indexName, err)
			}

			prefix := fmt.Sprintf("%s [%s]", test.Name, indexName)
			compareStructSlices(t, prefix, "column", "column(s)", columns, got)
		}
	}
}

func TestListIndexColumnsAux(t *testing.T) {
	testWithDB(t, testListIndexColumnsAux)
}

func testListIndexColumnsAux(t *testing.T, db *sql.DB) {

	data := []struct {
		Name    string
		SQL     []string
		Columns map[string][]pragma.IndexColumn
	}{
		{
			// Potentially fragile test.
			// Are auxiliary index columns stable?
			Name: "Basic Index",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					v COLLATE NOCASE,
					w,
					x,
					y,
					z
				)`,
				`CREATE INDEX idx1 ON a(v DESC)`,
				`CREATE INDEX idx2 ON a(w COLLATE NOCASE DESC,z)`,
				`CREATE INDEX idx3 ON a(v+w,x,y+z)`,
			},
			Columns: map[string][]pragma.IndexColumn{
				"xxxxxx": nil,
				"idx1": {
					{
						Name:       nullString("v"),
						Rank:       0,
						TableRank:  0,
						Descending: true,
						Collation:  "NOCASE",
						IsKey:      true,
					},
					{
						Rank:      1,
						TableRank: pragma.TableRankRowID,
						Collation: "BINARY",
					},
				},
				"idx2": {
					{
						Name:       nullString("w"),
						Rank:       0,
						TableRank:  1,
						Descending: true,
						Collation:  "NOCASE",
						IsKey:      true,
					},
					{
						Name:      nullString("z"),
						Rank:      1,
						TableRank: 4,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Rank:      2,
						TableRank: pragma.TableRankRowID,
						Collation: "BINARY",
					},
				},
				"idx3": {
					{
						Rank:      0,
						TableRank: pragma.TableRankExpression,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Name:      nullString("x"),
						Rank:      1,
						TableRank: 2,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Rank:      2,
						TableRank: pragma.TableRankExpression,
						Collation: "BINARY",
						IsKey:     true,
					},
					{
						Rank:      3,
						TableRank: pragma.TableRankRowID,
						Collation: "BINARY",
					},
				},
			},
		},
	}

	for _, test := range data {

		execute(t, db, test.SQL)

		for indexName, columns := range test.Columns {

			got, err := pragma.ListIndexColumnsAux(db, indexName)
			if err != nil {
				t.Fatalf("ListIndexColumns(%q) returned error %s", indexName, err)
			}

			prefix := fmt.Sprintf("%s [%s]", test.Name, indexName)
			compareStructSlices(t, prefix, "column", "column(s)", columns, got)
		}
	}
}

func TestListColumns(t *testing.T) {
	testWithDB(t, testListColumns)
}

func testListColumns(t *testing.T, db *sql.DB) {

	data := []struct {
		Name      string
		TableName string
		SQL       []string
		Columns   []pragma.Column
	}{
		{
			Name:      "Basic Table",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					w,
					x INTEGER,
					y INTEGER NOT NULL,
					z INTEGER NOT NULL DEFAULT 42
				)`,
			},
			Columns: []pragma.Column{
				{
					ID:   0,
					Name: "w",
				},
				{
					ID:   1,
					Name: "x",
					Type: "INTEGER",
				},
				{
					ID:      2,
					Name:    "y",
					Type:    "INTEGER",
					NotNull: true,
				},
				{
					ID:      3,
					Name:    "z",
					Type:    "INTEGER",
					NotNull: true,
					Default: []byte{'4', '2'},
				},
			},
		},
		{
			Name:      "Aliased ROWID Table",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x INTEGER PRIMARY KEY,
					y,
					z DEFAULT CURRENT_TIMESTAMP
				)`,
			},
			Columns: []pragma.Column{
				{
					ID:         0,
					Name:       "x",
					Type:       "INTEGER",
					PrimaryKey: 1,
				},
				{
					ID:   1,
					Name: "y",
				},
				{
					ID:      2,
					Name:    "z",
					Default: []byte("CURRENT_TIMESTAMP"),
				},
			},
		},
		{
			Name:      "Without ROWID Table",
			TableName: "a",
			SQL: []string{
				`DROP TABLE IF EXISTS a`,
				`CREATE TABLE a (
					x,
					y,
					z,
					PRIMARY KEY (z,y)
				) WITHOUT ROWID`,
			},
			Columns: []pragma.Column{
				{
					ID:   0,
					Name: "x",
				},
				{
					ID:   1,
					Name: "y",
					// Sqlite forces Primary Key columns in WITHOUT ROWID
					// tables to be NOTNULL.
					NotNull:    true,
					PrimaryKey: 2,
				},
				{
					ID:   2,
					Name: "z",
					// Sqlite forces Primary Key columns in WITHOUT ROWID
					// tables to be NOTNULL.
					NotNull:    true,
					PrimaryKey: 1,
				},
			},
		},
	}

	for _, test := range data {

		execute(t, db, test.SQL)

		got, err := pragma.ListColumns(db, test.TableName)
		if err != nil {
			t.Fatalf("ListColumns(%q) returned error %q", test.TableName, err)
		}

		compareStructSlices(t, test.Name, "column", "column(s)", test.Columns, got)
	}
}

func TestRunForeignKeyCheck(t *testing.T) {
	testWithDB(t, testRunForeignKeyCheck)
}

func testRunForeignKeyCheck(t *testing.T, db *sql.DB) {

	// TODO: Why do we get spurious ChildRowID values for
	// WITHOUT ROWID tables and not NULL as stated in the
	// SQLite docs?.

	sql := []string{

		`DROP TABLE IF EXISTS groups`,
		`DROP TABLE IF EXISTS labels`,
		`DROP TABLE IF EXISTS people`,
		`DROP TABLE IF EXISTS albums`,

		`CREATE TABLE groups (
			id   INTEGER PRIMARY KEY,
			name TEXT
		)`,

		`CREATE TABLE labels (
			name TEXT PRIMARY KEY
		) WITHOUT ROWID`,

		`CREATE TABLE people (
			name    TEXT,
			groupid INTEGER REFERENCES groups
		)`,

		`CREATE TABLE albums (
			name    TEXT,
			groupid INTEGER REFERENCES groups,
			label   TEXT REFERENCES labels,
			PRIMARY KEY(name)
		) WITHOUT ROWID`,

		`INSERT INTO groups(name) VALUES('The Beatles')`,

		`INSERT INTO labels(name) VALUES('Parlophone')`,

		`INSERT INTO people(groupid, name) VALUES(1, 'Paul McCartney')`,
		`INSERT INTO people(groupid, name) VALUES(1, 'John Lennon')`,
		`INSERT INTO people(groupid, name) VALUES(0, 'George Harrison')`,
		`INSERT INTO people(name) VALUES('Ringo Starr')`,
		`INSERT INTO people(groupid, name) VALUES(-1, 'Pete Best')`,

		`INSERT INTO albums(groupid, label, name) VALUES(1, 'Parlophone', 'Rubber Soul')`,
		`INSERT INTO albums(groupid, label, name) VALUES(1, 'Parlophone', 'Revolver')`,
		//`INSERT INTO albums(groupid, label, name) VALUES(1, 'Apple', 'Abbey Road')`,
		//`INSERT INTO albums(groupid, label, name) VALUES(2, 'Parlophone', 'McCartney II')`,
		//`INSERT INTO albums(groupid, label, name) VALUES(3, 'Apple', 'Imagine')`,
	}

	data := []struct {
		Name       string
		TableName  string
		Violations []pragma.ForeignKeyViolation
	}{
		{
			Name:      "Parent Table",
			TableName: "groups",
		},
		{
			Name:      "Child Table",
			TableName: "people",
			Violations: []pragma.ForeignKeyViolation{
				{
					ParentTable:  "groups",
					ChildTable:   "people",
					ChildRowID:   nullInt(3),
					ForeignKeyID: 0,
				},
				{
					ParentTable:  "groups",
					ChildTable:   "people",
					ChildRowID:   nullInt(5),
					ForeignKeyID: 0,
				},
			},
		},
		{
			Name:       "Child Table WITHOUT ROWID",
			TableName:  "albums",
			Violations: []pragma.ForeignKeyViolation{
			/*
				{
					ParentTable: "labels",
					ChildTable:  "albums",
					// ChildRowID should be NULL, got 19
					ForeignKeyID: 0,
				},
				{
					ParentTable: "labels",
					ChildTable:  "albums",
					// ChildRowID should be NULL, got 17
					ForeignKeyID: 0,
				},
				{
					ParentTable: "groups",
					ChildTable:  "albums",
					// ChildRowID should be NULL, got 17
					ForeignKeyID: 1,
				},
				{
					ParentTable: "groups",
					ChildTable:  "albums",
					// ChildRowID should be NULL, got 27
					ForeignKeyID: 1,
				},
			*/
			},
		},
		{
			Name: "All Tables",
			Violations: []pragma.ForeignKeyViolation{
				{
					ParentTable:  "groups",
					ChildTable:   "people",
					ChildRowID:   nullInt(3),
					ForeignKeyID: 0,
				},
				{
					ParentTable:  "groups",
					ChildTable:   "people",
					ChildRowID:   nullInt(5),
					ForeignKeyID: 0,
				},
			},
		},
	}

	if err := pragma.SetForeignKeys(db, false); err != nil {
		t.Fatalf("SetForeignKeys(false) returned error %s", err)
	}

	execute(t, db, sql)

	for _, test := range data {

		got, err := pragma.RunForeignKeyCheck(db, test.TableName)
		if err != nil {
			t.Fatalf("RunForeignKeyCheck(%q) returned error %s", test.TableName, err)
		}

		compareStructSlices(t, test.Name, "violation", "violation(s)", test.Violations, got)
	}
}

// Test operations

func TestRunIncrementalVacuum(t *testing.T) {
	testWithDB(t, testRunIncrementalVacuum)
}

func testRunIncrementalVacuum(t *testing.T, db *sql.DB) {

	size := pragma.PageSize512

	if err := pragma.SetPageSize(db, size); err != nil {
		t.Fatalf("SetPageSize(%d) returned error: %s", int(size), err)
	}

	if err := pragma.SetAutoVacuum(db, pragma.AutoVacuumIncremental); err != nil {
		t.Fatalf("SetAutoVacuum(%d) returned error: %s", int(pragma.AutoVacuumIncremental), err)
	}

	execute(t, db, []string{
		`DROP TABLE IF EXISTS a`,
		`CREATE TABLE a (
			text TEXT
		)`,
	})

	executeMult(t, db, 10, `INSERT INTO a VALUES(?)`, strings.Repeat("X", int(size)))

	execute(t, db, []string{
		`DELETE FROM a`,
	})

	free, err := pragma.FreelistCount(db)
	if err != nil {
		t.Fatalf("FreelistCount returned error %s", err)
	}

	if free <= 0 {
		t.Fatalf("No pages in freelist")
	}

	for free > 0 {

		t.Logf("Pages in freelist: %d", free)

		// TODO: Why does RunIncrementalVacuum only remove one
		// freelist page, no matter how many pages we pass to
		// RunIncrementalVacuum?
		err := pragma.RunIncrementalVacuum(db, 5)
		if err != nil {
			t.Fatalf("RunIncrementalVacuum returned error %s", err)
		}

		f, err := pragma.FreelistCount(db)
		if err != nil {
			t.Fatalf("FreelistCount returned error %s", err)
		}

		if f >= free {
			t.Errorf("Expected new freelist count to be less than %d, got %d", free, f)
			break
		}

		free = f
	}
}

func TestRunIntegrityCheck(t *testing.T) {
	testWithDB(t, func(t *testing.T, db *sql.DB) {
		testRunIntegrityCheck(t, db, "RunIntegrityCheck", pragma.RunIntegrityCheck)
	})
}

func TestRunQuickCheck(t *testing.T) {
	testWithDB(t, func(t *testing.T, db *sql.DB) {
		testRunIntegrityCheck(t, db, "RunQuickCheck", pragma.RunQuickCheck)
	})
}

func testRunIntegrityCheck(t *testing.T, db *sql.DB, name string, check func(*sql.DB, int) ([]string, error)) {

	// TODO: Why don't integrity checks pick up CHECK constraint
	// violations?

	// Run a check on the empty database.

	errors, err := check(db, 100)
	if err != nil {
		t.Fatalf("%s returned error %s", name, err)
	}

	for _, s := range errors {
		t.Errorf("Unexpected %s error on empty database: %s", name, s)
	}

	// Add some dodgy data.

	err = pragma.SetCheckConstraints(db, false)
	if err != nil {
		t.Fatalf("SetCheckContraints(false) returned error %s", err)
	}

	execute(t, db, []string{
		`DROP TABLE IF EXISTS a`,
		`CREATE TABLE a (
			x INTEGER CHECK (x > 0)
		)`,
		`INSERT INTO a(x) VALUES(0)`,
		`INSERT INTO a(x) VALUES(100)`,
		`INSERT INTO a(x) VALUES(-100)`,
	})

	// Re-run integrity check.

	errors, err = check(db, 100)
	if err != nil {
		t.Fatalf("%s returned error %s", name, err)
	}

	for _, s := range errors {
		t.Logf("Got %s error (CHECK constraints OFF): %s", name, s)
	}

	// Re-run integrity check with CHECK constraints enforced.

	err = pragma.SetCheckConstraints(db, true)
	if err != nil {
		t.Fatalf("SetCheckContraints(true) returned error %s", err)
	}

	errors, err = check(db, 100)
	if err != nil {
		t.Fatalf("%s returned error %s", name, err)
	}

	for _, s := range errors {
		t.Logf("Got %s error (CHECK constraints ON): %s", name, s)
	}
}

func TestRunWALCheckpoint(t *testing.T) {

	if testFlags.InMemory {
		// Skip test for in-memory databases. RunWalCheckpoint
		// always returns an error.
		t.SkipNow()
	}

	testWithDB(t, testRunWALCheckpoint)
}

func testRunWALCheckpoint(t *testing.T, db *sql.DB) {

	size := pragma.PageSize512

	err := pragma.SetPageSize(db, size)
	if err != nil {
		t.Fatalf("SetPageSize returned error %s", err)
	}

	// A WAL checkpoint in non-WAL mode is a no-op.

	err = pragma.SetJournalMode(db, pragma.JournalModeDelete)
	if err != nil {
		t.Fatalf("SetJournalMode returned error %s", err)
	}

	_, _, err = pragma.RunWALCheckpoint(db, pragma.WALCheckpointPassive)
	if err != pragma.ErrWALCheckpointFailed {
		t.Fatalf("RunWalCheckpoint returned error %s", err)
	}

	// Switch to WAL mode and populate the WAL by inserting
	// some data.

	err = pragma.SetJournalMode(db, pragma.JournalModeWAL)
	if err != nil {
		t.Fatalf("SetJournalMode returned error %s", err)
	}

	execute(t, db, []string{
		`DROP TABLE IF EXISTS a`,
		`CREATE TABLE a (
			text TEXT
		)`,
	})

	executeMult(t, db, 10, `INSERT INTO a VALUES(?)`, strings.Repeat("X", int(size)))

	// This will return the number of pages in the WAL, and
	// the number of pages moved over to the database.
	total, checked, err := pragma.RunWALCheckpoint(db, pragma.WALCheckpointPassive)
	if err != nil {
		t.Fatalf("RunWalCheckpoint(PASSIVE) returned error %s", err)
	}

	if total <= 0 {
		t.Fatalf("RunWALCheckpoint returned %d pages in the WAL", total)
	}

	if checked != total {
		t.Fatalf("RunWALCheckpoint reported %d pages in the WAL, but only wrote %d", total, checked)
	}

	// Re-run the checkpoint in TRUNCATE mode. This should clear
	// the WAL.
	total, _, err = pragma.RunWALCheckpoint(db, pragma.WALCheckpointTruncate)
	if err != nil {
		t.Fatalf("RunWalCheckpoint(TRUNCATE) returned error %s", err)
	}

	if total != 0 {
		t.Fatalf("RunWALCheckpoint(TRUNCATE) left %d pages in the WAL", total)
	}
}

func TestSetCaseSensitiveLike(t *testing.T) {
	testWithDB(t, testSetCaseSensitiveLike)
}

func testSetCaseSensitiveLike(t *testing.T, db *sql.DB) {

	execute(t, db, []string{
		`DROP TABLE IF EXISTS a`,
		`CREATE TABLE a (
			value TEXT
		)`,
		`INSERT INTO a VALUES('some text in lowercase')`,
	})

	var count int
	var caseSensitive bool

	q := `SELECT COUNT(*) FROM a WHERE value LIKE "%Text%"`

	for i := 0; i < 10; i++ {

		if err := pragma.SetCaseSensitiveLike(db, caseSensitive); err != nil {
			t.Fatalf("SetCaseSensitiveLike(%v) returned error %s", caseSensitive, err)
		}

		if err := db.QueryRow(q).Scan(&count); err != nil {
			t.Fatalf("Query %q returned error %s", q, err)
		}

		if caseSensitive && count != 0 {
			t.Fatalf("Expected row count to be 0, got %d", count)
		}

		if !caseSensitive && count != 1 {
			t.Fatalf("Expected row count to be 1, got %d", count)
		}

		caseSensitive = !caseSensitive
	}
}

func TestSetReadOnly(t *testing.T) {
	testWithDB(t, testSetReadOnly)
}

func testSetReadOnly(t *testing.T, db *sql.DB) {

	execute(t, db, []string{
		`DROP TABLE IF EXISTS a`,
		`CREATE TABLE a (
			num INTEGER
		)`,
		`INSERT INTO a VALUES(0)`,
	})

	var readOnly bool

	for i := 0; i < 10; i++ {

		if err := pragma.SetReadOnly(db, readOnly); err != nil {
			t.Fatalf("SetReadOnly(%v) returned error %s", readOnly, err)
		}

		_, err := db.Exec(`UPDATE a SET num = ?`, i)

		if readOnly && !isReadOnlyErr(err) {
			t.Fatalf("Expected a read-only error, got %v", err)
		}

		if !readOnly && err != nil {
			t.Fatalf("Expected a Nil error, got %v", err)
		}

		readOnly = !readOnly
	}
}

// Helpers

func compareStructSlices(t *testing.T, prefix, thing, things string, exp, got interface{}) {

	vexp := reflect.ValueOf(exp)
	vgot := reflect.ValueOf(got)

	if vgot.Type() != vexp.Type() {
		t.Fatalf("%s: Cannot compare different types %v and %v", prefix, vexp.Type(), vgot.Type())
	}

	if vgot.Len() != vexp.Len() {
		t.Errorf("%s: Expected %d %s, got %d", prefix, vexp.Len(), things, vgot.Len())
		return
	}

	numfields := vgot.Type().Elem().NumField()

	for i := 0; i < vgot.Len(); i++ {
		for j := 0; j < numfields; j++ {

			g := vgot.Index(i).Field(j).Interface()
			e := vexp.Index(i).Field(j).Interface()

			if !reflect.DeepEqual(g, e) {
				t.Errorf("%s: Expected %s %d to have %s %v, got %v", prefix, thing, i+1, vgot.Index(i).Type().Field(j).Name, stringify(e), stringify(g))
			}
		}
	}
}

func execute(t *testing.T, db *sql.DB, sql []string) {
	for _, q := range sql {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("db.Exec %q returned error %q", q, err)
		}
	}
}

func executeMult(t *testing.T, db *sql.DB, times int, q string, args ...interface{}) {

	stmt, err := db.Prepare(q)
	if err != nil {
		t.Fatalf("db.Prepare %q returned error %q", q, err)
	}
	defer stmt.Close()

	for i := 0; i < times; i++ {
		if _, err := stmt.Exec(args...); err != nil {
			t.Fatalf("Stmt.Exec %q returned error %q", q, err)
		}
	}
}

func isReadOnlyErr(err error) bool {
	return hasSqliteErrno(err, sqlite3.ErrReadonly)
}

func hasSqliteErrno(err error, errno sqlite3.ErrNo) bool {
	switch e := err.(type) {
	case sqlite3.Error:
		return e.Code == errno
	default:
		return false
	}
}

func isLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	b := *pb
	return (b == 0x04)
}

func equalStringSlices(s1, s2 []string) bool {

	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func stringify(v interface{}) string {
	switch v := v.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func nullString(s string) sql.NullString {
	return sql.NullString{
		Valid:  true,
		String: s,
	}
}

func nullInt(i int64) sql.NullInt64 {
	return sql.NullInt64{
		Valid: true,
		Int64: i,
	}
}

func openDB(filename string, options map[string]string) (*sql.DB, error) {

	values := url.Values{}
	for k, v := range options {
		values.Set(k, v)
	}

	if params := values.Encode(); params != "" {
		filename += "?" + params
	}

	return sql.Open("sqlite3", "file:"+filename)
}

func inMemoryDB() (*sql.DB, func(), error) {

	db, err := openDB(":memory:", map[string]string{
		"cache": "shared",
	})
	if err != nil {
		return nil, nil, err
	}

	return db, func() {
		db.Close()
	}, nil
}

func tempFileDB() (*sql.DB, func(), error) {

	f, err := ioutil.TempFile("", "pragma-test")
	if err != nil {
		return nil, nil, err
	}

	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return nil, nil, err
	}

	db, err := openDB(f.Name(), nil)
	if err != nil {
		os.Remove(f.Name())
		return nil, nil, err
	}

	return db, func() {
		db.Close()
		os.Remove(f.Name())
	}, nil
}

func testWithDB(t *testing.T, fn func(t *testing.T, db *sql.DB)) {

	if newDB == nil {
		newDB = inMemoryDB
		if !testFlags.InMemory {
			newDB = tempFileDB
		}
	}

	db, close, err := newDB()
	if err != nil {
		t.Fatalf("Could not open db: %s", err)
	}
	defer close()
	fn(t, db)
}
