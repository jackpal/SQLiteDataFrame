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

/// An enum that can hold any sqlite column value.
public enum SQLiteValue {
  case null
  case int(Int64)
  case real(Double)
  case text(String)
  case blob(Data)
  
  init(statement:OpaquePointer, columnIndex: Int32) {
    let columnType = sqlite3_column_type(statement, columnIndex)
    switch columnType {
    case SQLITE_NULL:
      self = .null
    case SQLITE_INTEGER:
      self = .int(sqlite3_column_int64(statement, columnIndex))
    case SQLITE_FLOAT:
      self = .real(sqlite3_column_double(statement, columnIndex))
    case SQLITE_TEXT:
      self = .text(String(cString:sqlite3_column_text(statement, columnIndex)))
    case SQLITE_BLOB:
      self = .blob(Data(bytes:sqlite3_column_blob(statement, columnIndex),
                  count:Int(sqlite3_column_bytes(statement, columnIndex))))
    default:
      fatalError("Unknown column type \(columnType) at columnIndex \(columnIndex)")
    }
  }
}

/// A protocol that can convert a value to a SQLiteValue.
public protocol SQLiteEncodable {
  var sqliteValue: SQLiteValue { get }
}

/// A protocol that can convert a SQLiteValue to a value.
public protocol SQLiteDecodable {
  init?(statement:OpaquePointer, parameterIndex: Int32)
}

// https://stackoverflow.com/questions/45234233/why-cant-i-pass-a-protocol-type-to-a-generic-t-type-parameter

extension SQLiteDecodable {

  static func decodeSQL(statement:OpaquePointer, parameterIndex: Int32) -> SQLiteDecodable? {
    decodeSQLHelper(serviceType: self, statement: statement, parameterIndex: parameterIndex)
  }
  
  static func decodeSQLHelper<T>(serviceType: T.Type, statement:OpaquePointer, parameterIndex: Int32) -> SQLiteDecodable? where T: SQLiteDecodable {
    return T(statement: statement, parameterIndex: parameterIndex)
  }

}

public protocol SQLiteCodable : SQLiteEncodable, SQLiteDecodable {
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
   Intializes a DataFrame from a given SQLite database file and table name.
   
   - Parameter contentsOfSQLiteDatabaseFile: The sqlite3 database file.
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
   
   // Create a test database file.
   var db: OpaquePointer!
   _ = sqlite3_open("temp.db", &db)
   try check(sqlite3_exec(db, """
     create table tasks (
       description text not null,
       done bool default false not null
     );
     insert into tasks (description) values ('Walk dog');
     insert into tasks (description) values ('Drink milk');
     insert into tasks (description) values ('Write code');
""", nil, nil, nil))
   sqlite3_close(db)
   
   // Read the table into a data frame
   let dataFrame = try DataFrame(contentsOfSQLiteDatabaseFile:URL.fileURL(withPath:"temp.db"), table:"tasks")
   ```
   
   The DataFrame's column types are determined by the columns' declared types, using a modified version of the
   SQLite3 [Type Affinity](https://www.sqlite.org/datatype3.html) rules.
   If the column's type can't be determined, then the `.any` type is used.
   */
  init(contentsOfSQLiteDatabaseFile sqliteDatabaseFile: URL, table: String, columns: [String]? = nil,
       types: [String:SQLiteType]? = nil, capacity: Int = 0) throws {
    var db: OpaquePointer!
    _ = try sqliteDatabaseFile.withUnsafeFileSystemRepresentation{
      try check(sqlite3_open($0, &db))
    }
    defer { sqlite3_close(db) }
    try self.init(connection:db, table:table, columns:columns, types:types, capacity:capacity)
  }
  
  /**
   Intializes a DataFrame from a given SQLite table name.
   
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
   
   let dataFrame = try DataFrame(connection: db, table:"tasks")
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
   If the               column's type can't be determined, then the `.any` type is used.
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
    let chosenColumnTypes = chosenColumnIndecies.map {statementIndex -> SQLiteType in
      if let types = types {
        if let chosenType = types[columnNames[Int(statementIndex)]] {
          return chosenType
        }
      }
      if let declType = sqlite3_column_decltype(statement, statementIndex) {
        return SQLiteType(declaredType:String(cString:declType))
      }
      return SQLiteType(declaredType:"")
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
    try readSQL(statement: statement)
  }

  mutating func readSQL(connection: OpaquePointer, table: String) throws {
    let columnText = columns.map(\.name).joined(separator: ",")
    let statement = "SELECT \(columnText) FROM \(table);"
    try readSQL(connection: connection, statement: statement)
  }

  mutating func readSQL(connection: OpaquePointer, statement: String) throws {
    var preparedStatement: OpaquePointer!
    try check(sqlite3_prepare_v2(connection, statement,-1,&preparedStatement,nil))
    try readSQL(statement: preparedStatement)
  }
  
  mutating func readSQL(statement: OpaquePointer, finalizeStatement: Bool = true) throws {
    defer {
      if finalizeStatement {
        sqlite3_finalize(statement)
      }
    }
    
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
      for (col, column) in columns.enumerated() {
        let columnIndex = Int32(col)
        // Checking for SQLiteDecodable conformance before the nil check lets us have columns
        // that decode null as a value.
        if case let sqliteDecodableType as SQLiteDecodable.Type = column.wrappedElementType {
          rows[rowIndex][col] = sqliteDecodableType.decodeSQL(statement:statement, parameterIndex: columnIndex)
          continue
        }
        let sqlColumnType = sqlite3_column_type(statement, columnIndex)
        if sqlColumnType == SQLITE_NULL {
          continue
        }
        switch column.wrappedElementType {
        case is Bool.Type:
          rows[rowIndex][col] = sqlite3_column_int64(statement, columnIndex) != 0
        case is Int8.Type:
          rows[rowIndex][col] = Int8(sqlite3_column_int64(statement, columnIndex))
        case is Int16.Type:
          rows[rowIndex][col] = Int16(sqlite3_column_int64(statement, columnIndex))
        case is Int32.Type:
          rows[rowIndex][col] = Int32(sqlite3_column_int64(statement, columnIndex))
        case is Int64.Type:
          rows[rowIndex][col] = sqlite3_column_int64(statement, columnIndex)
        case is Int.Type:
          rows[rowIndex][col] = Int(sqlite3_column_int64(statement, columnIndex))
        case is UInt8.Type:
          rows[rowIndex][col] = UInt8(sqlite3_column_int64(statement, columnIndex))
        case is UInt16.Type:
          rows[rowIndex][col] = UInt16(sqlite3_column_int64(statement, columnIndex))
        case is UInt32.Type:
          rows[rowIndex][col] = UInt32(sqlite3_column_int64(statement, columnIndex))
        case is UInt64.Type:
          if sqlColumnType == SQLITE_TEXT {
            // This decodes text representation in case its > Int64.max
            rows[rowIndex][col] = UInt64(String(cString:sqlite3_column_text(statement, columnIndex))          )
          } else {
            rows[rowIndex][col] = UInt64(sqlite3_column_int64(statement, columnIndex))
          }
        case is UInt.Type:
          rows[rowIndex][col] = UInt(sqlite3_column_int64(statement, columnIndex))
        case is String.Type:
          rows[rowIndex][col] = String(cString:sqlite3_column_text(statement, columnIndex))
        case is Float.Type:
            rows[rowIndex][col] = Float(sqlite3_column_double(statement, columnIndex))
        case is Double.Type:
            rows[rowIndex][col] = Double(sqlite3_column_double(statement, columnIndex))
        case is Data.Type:
            rows[rowIndex][col] = Data(bytes:sqlite3_column_blob(statement, columnIndex),
                                           count:Int(sqlite3_column_bytes(statement, columnIndex)))
        case is Date.Type:
            // See "Date and Time Datatype" https://www.sqlite.org/datatype3.html
            // TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
            // REAL as Julian day numbers, the number of days since noon in Greenwich on November 24, 4714 B.C. according
            // to the proleptic Gregorian calendar.
            // INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
          switch SQLiteValue(statement:statement, columnIndex: columnIndex) {
          case let .text(s):
            let formatter = DateFormatter()
            formatter.dateFormat = "yyyy-MM-dd HH:mm:ss" //this is the sqlite's format
            rows[rowIndex][col] = formatter.date(from:s)
          case let .int(i):
            rows[rowIndex][col] = Date(timeIntervalSince1970:TimeInterval(i))
          case let .real(julianDay):
            let SECONDS_PER_DAY = 86400.0
            let JULIAN_DAY_OF_ZERO_UNIX_TIME = 2440587.5
            let unixTime = (julianDay - JULIAN_DAY_OF_ZERO_UNIX_TIME) * SECONDS_PER_DAY
            self.rows[rowIndex][col] = Date(timeIntervalSince1970:TimeInterval(unixTime))
          default:
            break
          }
        default:
          if column.wrappedElementType == Any.self {
            switch SQLiteValue(statement:statement, columnIndex: columnIndex) {
            case .null:
              break
            case let .int(i):
                self.rows[rowIndex][col] = i
            case let .real(d):
                self.rows[rowIndex][col] = d
            case let .text(s):
                self.rows[rowIndex][col] = s
            case let .blob(d):
              self.rows[rowIndex][col] = d
            }
          }
        }
      }
      rowIndex += 1
    }
  }
  
  /**
   Write a table to a sqlite prepared statement.
   
   */
  func writeSQL(statement: OpaquePointer, finalizeStatement: Bool = true) throws {
    defer {
      if finalizeStatement {
        sqlite3_finalize(statement)
      }
    }
    let columns = columns.prefix(Int(sqlite3_bind_parameter_count(statement)))
    for rowIndex in 0..<shape.rows {
      for (i, column) in columns.enumerated() {
        let positionalIndex = Int32(1 + i)
        guard let item = column[rowIndex] else {
          try check(sqlite3_bind_null(statement, positionalIndex))
          continue
        }
        try DataFrame.writeItem(statement:statement, positionalIndex:positionalIndex, item:item)
      }
      let rc = sqlite3_step(statement)
      if rc != SQLITE_DONE {
        throw NSError(domain: kDomain, code: -2, userInfo:["rc": NSNumber(value: rc),
                                                           "row": NSNumber(value: rowIndex)])
      }
      try check(sqlite3_reset(statement))
    }
  }
  
  private static func writeItem(statement: OpaquePointer, positionalIndex: Int32, item: Any) throws {
    let SQLITE_TRANSIENT = unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self)

    switch item {
    case let q as SQLiteEncodable:
      switch q.sqliteValue {
      case .null:
        try check(sqlite3_bind_null(statement, positionalIndex))
      case let .int(i):
        try check(sqlite3_bind_int64(statement, positionalIndex, i))
      case let .real(d):
        try check(sqlite3_bind_double(statement, positionalIndex, d))
      case let .text(s):
        try check(sqlite3_bind_text(statement, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
      case let .blob(d):
        try d.withUnsafeBytes {
          _ = try check(sqlite3_bind_blob64(statement, positionalIndex, $0.baseAddress, sqlite3_uint64($0.count), SQLITE_TRANSIENT))
        }
      }
    // These are hard coded rather than implemented as SQLiteEncodable so that the user can override them.
    case let b as Bool:
      try check(sqlite3_bind_int(statement, positionalIndex, Int32(b ? 1 : 0)))
    case let i as Int8:
      try check(sqlite3_bind_int(statement, positionalIndex, Int32(i)))
    case let i as Int16:
      try check(sqlite3_bind_int(statement, positionalIndex, Int32(i)))
    case let i as Int32:
      try check(sqlite3_bind_int(statement, positionalIndex, Int32(i)))
    case let i as Int64:
      try check(sqlite3_bind_int64(statement, positionalIndex, Int64(i)))
    case let i as Int:
      try check(sqlite3_bind_int64(statement, positionalIndex, Int64(i)))
    case let i as UInt8:
      try check(sqlite3_bind_int(statement, positionalIndex, Int32(i)))
    case let i as UInt16:
      try check(sqlite3_bind_int(statement, positionalIndex, Int32(i)))
    case let i as UInt32:
      try check(sqlite3_bind_int64(statement, positionalIndex, Int64(i)))
    case let i as UInt64:
      if i <= UInt64(Int64.max) {
        try check(sqlite3_bind_int64(statement, positionalIndex, Int64(i)))
      } else {
        // It's better to preserve the data at the cost of strings vs writing nil or asserting.
        try check(sqlite3_bind_text(statement, positionalIndex, String(i).cString(using: .utf8),-1,SQLITE_TRANSIENT))
      }
    case let f as Float:
      try check(sqlite3_bind_double(statement, positionalIndex, Double(f)))
    case let f as CGFloat:
      try check(sqlite3_bind_double(statement, positionalIndex, Double(f)))
    case let d as Double:
      try check(sqlite3_bind_double(statement, positionalIndex, d))
    case let s as String:
      try check(sqlite3_bind_text(statement, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
    case let d as Date:
      let formatter = DateFormatter()
      formatter.dateFormat = "yyyy-MM-dd HH:mm:ss" //this is the sqlite's format.
      let dateString = formatter.string(from: d)
      try check(sqlite3_bind_text(statement, positionalIndex, dateString.cString(using: .utf8),-1,SQLITE_TRANSIENT))
    // Backup
    case let csc as CustomStringConvertible:
      let s = csc.description
      try check(sqlite3_bind_text(statement, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
    default:
      let s = String(reflecting:item)
      try check(sqlite3_bind_text(statement, positionalIndex, s.cString(using: .utf8),-1,SQLITE_TRANSIENT))
    }

  }
  
  func writeSQL(connection: OpaquePointer, statement: String) throws {
    var preparedStatement: OpaquePointer!
    try check(sqlite3_prepare_v2(connection, statement,-1,&preparedStatement,nil))
    try writeSQL(statement:preparedStatement)
  }

  func writeSQL(connection: OpaquePointer, table: String) throws {
    // Drop the table if it exists
    try check(sqlite3_exec(connection, "drop table if exists \(table)", nil, nil, nil))
    let columnDefs = columns.map {column -> String in
      let name = column.name
      var sqlType: String?
      switch column.wrappedElementType {
      case is String.Type:
        sqlType = "TEXT"
      case is Bool.Type:
        sqlType = "BOOLEAN"
      case is Int8.Type, is Int16.Type, is Int32.Type, is Int64.Type, is Int.Type,
        is UInt8.Type, is UInt16.Type, is UInt32.Type, is UInt64.Type, is UInt.Type:
        sqlType = "INT"
      case is Float.Type:
        sqlType = "FLOAT"
      case is Double.Type:
        sqlType = "DOUBLE"
      case is Date.Type:
        sqlType = "DATE"
      case is Data.Type:
        sqlType = "BLOB"
      default:
        break
      }
      if let sqlType = sqlType {
        return "\(name) \(sqlType)"
      }
      return name
    }
    let columnSpec = columnDefs.joined(separator: ",")
    let create = "create table \(table) (\(columnSpec))"
    try check(sqlite3_exec(connection, create, nil, nil, nil))
    let questionMarks = Array(repeating:"?", count:shape.columns).joined(separator: ",")
    let statement = "insert into \(table) values (\(questionMarks))"
    try writeSQL(connection:connection, statement: statement)
  }
  
  func writeSQL(file: URL, statement: String) throws {
    var db: OpaquePointer!
    _ = try file.withUnsafeFileSystemRepresentation{
      try check(sqlite3_open($0, &db))
    }
    defer { sqlite3_close(db) }
    try writeSQL(connection:db, statement: statement)
  }
  
  func writeSQL(file: URL, table: String) throws {
    var db: OpaquePointer!
    _ = try file.withUnsafeFileSystemRepresentation{
      try check(sqlite3_open($0, &db))
    }
    defer { sqlite3_close(db) }
    try writeSQL(connection: db, table: table)
  }

}
