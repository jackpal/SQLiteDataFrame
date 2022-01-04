# SQLiteDataFrame

Extends the [Tablular Data](https://developer.apple.com/documentation/tabulardata)
[DataFrame](https://developer.apple.com/documentation/tabulardata/dataframe)
struct to read the contents of a SQLite prepared statement into a DataFrame.

## Usage

```swift
   // Error checking omitted for brevity.
   
   // Create some SQL data.
   var db: OpaquePointer!
   _ = sqlite3_open(":memory:", &db)
   defer { sqlite3_close(db) }
   try check(sqlite3_exec(db, """
     create table tasks (
       description text not null,
       done bool default false not null,
       date DATE default CURRENT_TIMESTAMP not null
     );
     insert into tasks (description) values ('Walk dog');
     insert into tasks (description) values ('Drink milk');
     insert into tasks (description) values ('Write code');
""", nil, nil, nil))

   // Create a DataFrame from the results of the select statement.
   
   let dataFrame = try DataFrame(connection: db, statement:"select rowid, description, done, date from tasks order by rowid;")
   
   // Print the dataFrame, just for fun
   print(dataFrame)
   
   // Prints:
   //   ┏━━━┳━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
   //   ┃   ┃ rowid ┃ description ┃ done   ┃ date                      ┃
   //   ┃   ┃ <Int> ┃ <String>    ┃ <Bool> ┃ <Date>                    ┃
   //   ┡━━━╇━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
   //   │ 0 │     1 │ Walk dog    │ false  │ 2022-01-04 15:30:12 +0000 │
   //   │ 1 │     2 │ Drink milk  │ false  │ 2022-01-04 15:30:12 +0000 │
   //   │ 2 │     3 │ Write code  │ false  │ 2022-01-04 15:30:12 +0000 │
   //   └───┴───────┴─────────────┴────────┴───────────────────────────┘

```

## Features

- Uses with the low level Sqlite3 API. Should be compatible with any sqlite wrapper library.
- Works with:
  - A whole table specified by database file and table name.
  - A whole table specified by database connection and name.
  - A SELECT statement specified by database connection and String.
  - A select statement specified by a prepared sqlite3 statement.
- Automatically determines the column types based on the SQLite column declarations.
  - Recognizes the standard SQL column types using the [Affinity Rules](https://www.sqlite.org/datatype3.html):
    - Int
    - Double
    - String
    - Blob
  - Recognizes extended types:
    - Bool
    - Date
  - Columns whose types can't be determined are given type Any
  - You can manually override the default types by using the "types:" parameter.

## Limitations

- DataFrames do not support the concept of non-nullable types. Non-nullable sqlite columns will be represented in the DataFrame using nullable columns.

## ToDo

- Add helper methods for DataFrame-based CREATE TABLE, INSERT, UPDATE and DELETE.
