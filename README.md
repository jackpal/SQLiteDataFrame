# SQLiteDataFrame

Extends the [Tablular Data](https://developer.apple.com/documentation/tabulardata)
[DataFrame](https://developer.apple.com/documentation/tabulardata/dataframe)
struct to read the contents of a SQLite prepared statement into a DataFrame.

## Usage

```swift
   import SQLiteDataFrame
   import TabularData
   
   // Create some SQL data.
   var db: OpaquePointer!
   defer { sqlite3_close(db) }
   try checkSQLite(sqlite3_open(":memory:", &db))
   try checkSQLite(sqlite3_exec(db, """
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

- Creates TabularData DataFrames from SQL.
- Writes TabularData DataFrames to SQL.
- Uses the low level Sqlite3 API. Should be compatible with any sqlite wrapper library.
- Works with:
  - A whole table.
  - A SQL statement specified by a String.
  - A prepared sqlite3 statement.

## Details of Type mapping

- DataFrames do not support the concept of non-nullable types. Non-nullable sqlite columns will be represented in the DataFrame using nullable columns.

When creating a DataFrame, the DataFrame column types are automatically determined
  based on the SQLite column declarations.
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
  - You can control the encode/decode of a type by implementing the SQLiteEncodable / SQLiteDecodable protocols.

