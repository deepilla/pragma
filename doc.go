/*
Package pragma provides access to SQLite PRAGMA operations.

Pragmas are special SQLite commands that allow clients to
interact with database properties and metadata, as well as
perform basic administrative tasks. See
https://sqlite.org/pragma.html for more details.

Functions in this package fall into one of three categories:

1. Tasks

Task functions have the prefix "Run", e.g. RunIncrementalVacuum.
They perform (potentially long-running) operations on the
database and may alter its contents.

2. Lists

List functions have the prefix "List", e.g. ListIndexColumns.
They retrieve metadata from the database but do not alter
its contents.

3. Properties

Property functions update or query SQLite database settings.
Update functions have the prefix "Set", e.g. SetForeignKeys.
They modify the behaviour of SQLite but usually don't change
the database itself (there are a few exceptions). Query
functions don't have a particular prefix -- they're named
after the property, e.g. ForeignKeys.

Schemas

In SQLite, a single database connection can access multiple
databases, each with their own schemas and data. The Schema
type represents an individual database on a connection.

Many of the top-level functions in this package have matching
methods on the Schema type. Use the Schema methods to restrict
operation to a particular database. Unless stated otherwise,
the top-level functions will operate on all databases attached
to the database connection.
*/
package pragma
