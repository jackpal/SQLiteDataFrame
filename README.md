# SQLiteDataFrame

Extends the [Tablular Data](https://developer.apple.com/documentation/tabulardata)
[DataFrame](https://developer.apple.com/documentation/tabulardata/dataframe)
struct to read the contents of a SQLite prepared statement into a DataFrame.

## Usage

```swift
   // Error checking omitted for brevity.
   
   var db: OpaquePointer!
   _ = sqlite3_open(":memory:", &db)
   defer { sqlite3_close(db) }
   try check(sqlite3_exec(db, """
     create table tasks (
       description text not null,
       done bool default false not null
     );
     insert into tasks (description) values ('Walk dog');
     insert into tasks (description) values ('Drink milk');
     insert into tasks (description) values ('Write code');
""", nil, nil, nil))
   var statement: OpaquePointer!
   _ = sqlite3_prepare_v2(db,
       "select rowid, description, done from tasks order by rowid;",-1,&statement,nil)
   
   let dataFrame = try DataFrame(statement:statement)
   print(dataFrame)
```

## Features

- Follows DataFrame conventions.
- Works with:
  - Whole tables
  - SELECT statements.
  - Prepared statements.
- Automatically determins column types based on the SQLite column declarations.
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
