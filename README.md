# pragma

[![GoDoc](https://godoc.org/github.com/deepilla/pragma?status.svg)](https://godoc.org/github.com/deepilla/pragma)
[![Build Status](https://travis-ci.org/deepilla/pragma.svg?branch=master)](https://travis-ci.org/deepilla/pragma)
[![Go Report Card](https://goreportcard.com/badge/github.com/deepilla/pragma)](https://goreportcard.com/report/github.com/deepilla/pragma)

Pragma is a Go library that provides access to SQLite's [PRAGMA operations](https://sqlite.org/pragma.html). Pragmas are special SQLite commands that allow clients to interact with database properties and metadata, as well as perform basic administrative tasks.

## Installation

    go get github.com/deepilla/pragma

## Usage

Import the [database/sql](https://golang.org/pkg/database/sql/) package and an SQLite database driver.

    import "database/sql"
    import _ "github.com/mattn/go-sqlite3"

Import the pragma package.

    import "github.com/deepilla/pragma"

Open a database connection and pass it to the pragma functions.

```go
db, err := 	sql.Open("sqlite3", "path/to/sqlite.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

indexes, err := pragma.ListIndexes(db, "")
if err != nil {
    log.Fatal(err)
}

for _, idx := range indexes {
    fmt.Println("Found index", idx.Name, "on table", idx.Table)
}
```

## TODO

- Property functions should use code generation.
- Handle pragmas that return values when they are updated.
- Support more database drivers. So far only the [go-sqlite3 driver](https://github.com/mattn/go-sqlite3) seems to work. Other drivers cause failing tests and even panics (e.g, [gosqlite](https://github.com/gwenn/gosqlite), [sqlite](https://github.com/rsc/sqlite)).
- Investigate (and fix) tests that don't work as expected (see TODO comments in [pragma_test.go](pragma_test.go)).
- Flesh out property tests. Verifying that a database setting has been updated is fine, but we should also confirm that the database behaves as expected with the new setting.

## Further Reading

1. [pragma on GoDoc](https://godoc.org/github.com/deepilla/pragma)
2. [SQLite pragma docs](https://sqlite.org/pragma.html)

## Licensing

pragma is provided under an [MIT License](http://choosealicense.com/licenses/mit/). See the [LICENSE](LICENSE) file for details.
