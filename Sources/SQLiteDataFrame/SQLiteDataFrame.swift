import Foundation
import SQLite3
import TabularData

fileprivate let kDomain = "SQLiteDataFrame"

@discardableResult
fileprivate func check(_ code: Int32) throws -> Int32 {
  if code != SQLITE_OK && code != SQLITE_ROW && code != SQLITE_DONE {
    throw NSError(domain:kDomain, code:Int(code))
  }
  return code
}

/// An enhanced version of the SQLite column type.
public enum SQLiteType {
  case int
  case float
  case text
  case blob
  // Nonstandard
  case bool
  case date
  case any
  
  private static let affinityRules: [([String],Self)] = [
    (["INT"], .int),
    (["CHAR", "CLOB", "TEXT"], .text),
    (["BLOB"], .blob),
    (["REAL", "FLOA", "DOUB"], .float),
    // Nonstandard
    (["BOOL"], .bool),
    (["DATE"], .date)
  ]

  /// See [Type Affinity](https://www.sqlite.org/datatype3.html)
  init(declaredType:String) {
    let decl = declaredType.uppercased()
    for (substrings,type) in Self.affinityRules {
      for substring in substrings {
        if decl.contains(substring) {
          self = type
          return
        }
      }
    }
    self = .any
  }
}

public extension DataFrame {
  /**
   Intializes a DataFrame from a given SQLite statement.
   
   - Parameter connection: The sqlite3 database connection.
   - Parameter table: The sql table to read.
   - Parameter columns: An optional array of column names; Set to nil to use every column in the statement.
     For this particular initializer it is an error to specify column names that are not present in the table.
   - Parameter types; An optional dictionary of column names to `SQLiteType`s. The data frame infers the types for column names that aren’t in the dictionary.
   - Parameter capacity: The initial capacity of each column. It is normally fine to leave this as the default value.
   
   Columns in the columns parameter are used to create an internal SELECT statement. Columns
   which are not present in the table will cause an error.
   
   Columns in the types dictionary which are not present in the table will be ignored.
   
   Example:
   ```
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
   
   let dataFrame = try DataFrame(connection: db, statement:"select * from tasks order by rowid;")
   ```
   
   The DataFrame's column types are determined by the columns' declared types, using a modified version of the
   SQLite3 [Type Affinity](https://www.sqlite.org/datatype3.html) rules.
   If the column's type can't be determined, then the `.any` type is used.
   */
  init(connection: OpaquePointer, table: String, columns: [String]? = nil,
       types: [String:SQLiteType]? = nil, capacity: Int = 0) throws {
    let columnText = columns?.joined(separator: ",") ?? "*"
    let statement = "SELECT \(columnText) FROM \(table);"
    try self.init(connection:connection, statement:statement, columns:columns, types:types, capacity:capacity)
  }
  
  /**
   Intializes a DataFrame from a given SQLite statement.
   
   - Parameter connection: The sqlite database connection.
   - Parameter statement: The statement. The statement will be prepared and executed.
   - Parameter columns: An optional array of column names; Set to nil to use every column in the statement.
   - Parameter types; An optional dictionary of column names to `SQLiteType`s. The data frame infers the types for column names that aren’t in the dictionary.
   - Parameter capacity: The initial capacity of each column. It is normally fine to leave this as the default value.

   Columns in the columns parameter which are not returned by the select statement will be ignored.
   The columns parameter is provided for logical consistency with other DataFrame initiializers. However, it is
   inefficent to use this parameter, because the filtering is done after the sql data is fetched from the DB.
   Typically it is more efficient to filter by changing the `statement`.

   Columns in the types dictionary which are not returned by the select statement will be ignored.

   Example:
   ```
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
   
   let dataFrame = try DataFrame(connection: db, statement:"select * from tasks order by rowid;")
   ```
   
   The DataFrame's column types are determined by the columns' declared types, using a modified version of the
   SQLite3 [Type Affinity](https://www.sqlite.org/datatype3.html) rules.
   If the column's type can't be determined, then the `.any` type is used.
   */
  init(
    connection: OpaquePointer,
    statement: String,
    columns: [String]? = nil,
    types: [String:SQLiteType]? = nil,
    capacity: Int = 0
  ) throws {
    var preparedStatement: OpaquePointer!
    try check(sqlite3_prepare_v2(connection, statement, -1, &preparedStatement, nil))
    try self.init(statement:preparedStatement, columns:columns, types:types, capacity:capacity)
  }
  
  /**
   Intializes a DataFrame from a prepared statement.
   
   - Parameter statement: The prepared statement. The statement will be finalalized by the initializer.
   - Parameter columns: An optional array of column names; Set to nil to use every column in the statement.
   - Parameter types; An optional dictionary of column names to `SQLiteType`s. The data frame infers the types for column names that aren’t in the dictionary.
   - Parameter capacity: The initial capacity of each column. It is normally fine to leave this as the default value.
   
   Columns in the columns parameter which are not returned by the select statement will be ignored.
   The columns parameter is provided for logical consistency with other DataFrame initiializers. However, it is
   inefficent to use this parameter, because the filtering is done after the sql data is fetched from the DB.
   Typically it is more efficient to filter by changing the `statement`.

   Columns in the types dictionary which are not returned by the select statement will be ignored.

   Example:
   ```
   // Error checking omitted for brevity.
   
   var db: OpaquePointer!
   _ = sqlite3_open(":memory:", &db)
   defer { sqlite3_close(db) }
   _ = sqlite3_exec(db, """
     create table tasks (
       description text not null,
       done bool default false not null
     );
     insert into tasks (description) values ('Walk dog');
     insert into tasks (description) values ('Drink milk');
     insert into tasks (description) values ('Write code');
""", nil, nil, nil)
   var statement: OpaquePointer!
   _ = sqlite3_prepare_v2(db,
       "select rowid, description, done from tasks order by rowid;",-1,&statement,nil)
   
   let dataFrame = try DataFrame(statement:statement)
   ```
   
   The DataFrame's column types are determined by the columns' declared types, using a modified version of the
   SQLite3 [Type Affinity](https://www.sqlite.org/datatype3.html) rules.
   If the column's type can't be determined, then the `.any` type is used.
   */
  init(statement: OpaquePointer, columns: [String]? = nil,
       types: [String:SQLiteType]? = nil, capacity: Int = 0) throws {
    defer { sqlite3_finalize(statement) }
    
    let allowedColumns: Set<String>?
    if let columns = columns {
      allowedColumns = Set(columns)
    } else {
      allowedColumns = nil
    }
    
    let columnCount = sqlite3_column_count(statement)
    let columnNames = (0..<columnCount).map { String(cString:sqlite3_column_name(statement, $0)) }
    let chosenColumnIndecies = (0..<columnCount).filter { allowedColumns?.contains(columnNames[Int($0)]) ?? true }
    let chosenColumnTypes = chosenColumnIndecies.map {statementIndex in
      types?[columnNames[Int(statementIndex)]] ??
      SQLiteType(declaredType: String(cString:sqlite3_column_decltype(statement, statementIndex)))
    }
    let chosenColumnIndeciesAndTypes = zip(chosenColumnIndecies, chosenColumnTypes)
    let columns = chosenColumnIndeciesAndTypes.map {(columnIndex, columnType) -> AnyColumn in
      let columnName = String(cString:sqlite3_column_name(statement, columnIndex))
      switch columnType {
      case .int:
        return Column<Int>(name:columnName, capacity: capacity).eraseToAnyColumn()
      case .float:
        return Column<Double>(name:columnName, capacity: capacity).eraseToAnyColumn()
      case .text:
        return Column<String>(name:columnName, capacity: capacity).eraseToAnyColumn()
      case .blob:
        return Column<Data>(name:columnName, capacity: capacity).eraseToAnyColumn()
      case .bool:
        return Column<Bool>(name:columnName, capacity: capacity).eraseToAnyColumn()
      case .date:
        return Column<Date>(name:columnName, capacity: capacity).eraseToAnyColumn()
      case .any:
        return Column<Any>(name:columnName, capacity: capacity).eraseToAnyColumn()
      }
    }
    self.init(columns: columns)
    var rowIndex = 0
    while true {
      let rc = sqlite3_step(statement)
      if rc == SQLITE_DONE {
          break
      }
      if rc != SQLITE_ROW {
        throw NSError(domain: kDomain, code: -2, userInfo:["rc": NSNumber(value: rc),
                                                           "row": NSNumber(value: rowIndex)])
      }
      self.appendEmptyRow()
      chosenColumnIndeciesAndTypes.enumerated().forEach {(col, arg1) in
        let (columnIndex, columnType) = arg1
        let type = sqlite3_column_type(statement, columnIndex)
        if type != SQLITE_NULL {
          switch columnType {
          case .bool:
            self.rows[rowIndex][col] = sqlite3_column_int64(statement, columnIndex) != 0
          case .int:
            self.rows[rowIndex][col] = Int(sqlite3_column_int64(statement, columnIndex))

          case .float:
            self.rows[rowIndex][col] = sqlite3_column_double(statement, columnIndex)
          case .text:
            self.rows[rowIndex][col] = String(cString:sqlite3_column_text(statement, columnIndex))
          case .blob:
            self.rows[rowIndex][col] = Data(bytes:sqlite3_column_blob(statement, columnIndex),
                                           count:Int(sqlite3_column_bytes(statement, columnIndex)))
          case .date:
            // See "Date and Time Datatype" https://www.sqlite.org/datatype3.html
            // TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
            // REAL as Julian day numbers, the number of days since noon in Greenwich on November 24, 4714 B.C. according
            // to the proleptic Gregorian calendar.
            // INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
            switch type {
            case SQLITE_TEXT:
              let formatter = DateFormatter()
              formatter.dateFormat = "yyyy-MM-dd HH:mm:ss" //this is the sqlite's format
              self.rows[rowIndex][col] = formatter.date(from:String(cString:sqlite3_column_text(statement, columnIndex)))
            case SQLITE_INTEGER:
              self.rows[rowIndex][col] = Date(timeIntervalSince1970:
                                                TimeInterval(sqlite3_column_int64(statement, columnIndex)))
            case SQLITE_FLOAT:
              let SECONDS_PER_DAY = 86400.0
              let JULIAN_DAY_OF_ZERO_UNIX_TIME = 2440587.5
              let julianDay = sqlite3_column_double(statement, columnIndex)
              let unixTime = (julianDay - JULIAN_DAY_OF_ZERO_UNIX_TIME) * SECONDS_PER_DAY
              self.rows[rowIndex][col] = Date(timeIntervalSince1970:TimeInterval(unixTime))
            default:
              break
            }

          case .any:
            switch type {
            case SQLITE_INTEGER:
              self.rows[rowIndex][col] = Int(sqlite3_column_int64(statement, columnIndex))
            case SQLITE_FLOAT:
              self.rows[rowIndex][col] = sqlite3_column_double(statement, columnIndex)
            case SQLITE_TEXT:
              self.rows[rowIndex][col] = String(cString:sqlite3_column_text(statement, columnIndex))
            case SQLITE_BLOB:
              self.rows[rowIndex][col] = Data(bytes:sqlite3_column_blob(statement, columnIndex),
                                             count:Int(sqlite3_column_bytes(statement, columnIndex)))
            default:
              fatalError("Row {rowIndex} Column {i} Unknown type {type}")
            }
          }
        }
      }
      rowIndex += 1
    }
  }
}
