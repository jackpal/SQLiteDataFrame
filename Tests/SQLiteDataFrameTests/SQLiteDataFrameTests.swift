import SQLite3
import TabularData
import XCTest
@testable import SQLiteDataFrame

@discardableResult
fileprivate func check(_ code: Int32) throws -> Int32 {
  if code != SQLITE_OK && code != SQLITE_ROW && code != SQLITE_DONE {
    throw NSError(domain:"SQLiteDataFrameTests", code:Int(code))
  }
  return code
}

final class SQLiteDataFrameTests: XCTestCase {
  var db: OpaquePointer!
  
  override func setUpWithError() throws {
    try super.setUpWithError()
    
    try check(sqlite3_open(":memory:", &db))
    
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
  }
  
  override func tearDown() {
    sqlite3_close(db)
    super.tearDown()
  }
  
  func testDataFrame() throws {
    var statement: OpaquePointer!
    try check(sqlite3_prepare_v2(
      db, "select rowid, description, done, date from tasks order by rowid",-1,&statement,nil))
    let dataFrame = try DataFrame(statement:statement)
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,4)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testDataFrameFilterColumns() throws {
    var statement: OpaquePointer!
    try check(sqlite3_prepare_v2(
      db, "select rowid, description, done from tasks order by rowid",-1,&statement,nil))
    let dataFrame = try DataFrame(statement:statement, columns: ["bogus","description"])
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,1)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testDataFrameSpecifyTypes() throws {
    var statement: OpaquePointer!
    try check(sqlite3_prepare_v2(
      db, "select rowid, description, done from tasks order by rowid;",-1,&statement,nil))
    let dataFrame = try DataFrame(statement:statement,
                                  types: [
      "bogus":.bool,
      "description": .any,
      "done": .int
    ])
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,3)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testTextStatement() throws {
    let statement = "select description, date from tasks order by description"
    let dataFrame = try DataFrame(connection:db, statement:statement)
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,2)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testTable() throws {
    let dataFrame = try DataFrame(connection:db, table:"tasks")
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,3)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testTableSelectColumns() throws {
    let dataFrame = try DataFrame(connection:db, table:"tasks", columns:["description"])
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,1)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testReadTableFromDBFile() throws {
    let fileURL = URL(fileURLWithPath: "temp.db")
    defer { try? FileManager.default.removeItem(at: fileURL) }
    var db: OpaquePointer!
    try _ = fileURL.withUnsafeFileSystemRepresentation {
      try check(sqlite3_open($0, &db))
    }
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
    let dataFrame = try DataFrame(contentsOfSQLiteDatabaseFile:fileURL, table:"tasks")
    print(dataFrame)
  }
  
  func testWriteSQL() throws {
    var d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
      Column<Bool>(name:"done", capacity:0).eraseToAnyColumn(),
      Column<Int8>(name:"byte", capacity:0).eraseToAnyColumn(),
      Column<CGPoint>(name:"points", capacity:0).eraseToAnyColumn()
    ])
    d.append(row: "Pick up drycleaning", false, Int8(3), CGPoint(x: 1.0, y: 1.0))
    d.append(row: "Rake leaves", false, Int8(3), CGPoint(x: 2.0, y: 2.0))
    try print(DataFrame(csvData:d.csvRepresentation()))

    try d.writeSQL(connection:db, statement: "insert into tasks (description, done) values (?,?)")
    let tasks = try DataFrame(connection:db, table:"tasks")
    print(tasks)
    XCTAssertEqual(tasks.shape.rows, 5)
  }
  
  func testWriteSQLTable() throws {
    var d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
      Column<Bool>(name:"done", capacity:0).eraseToAnyColumn(),
      Column<Int8>(name:"byte", capacity:0).eraseToAnyColumn(),
      Column<CGPoint>(name:"points", capacity:0).eraseToAnyColumn(),
      Column<IntThing>(name:"thing", capacity:0).eraseToAnyColumn()
    ])
    d.append(row: "Pick up drycleaning", false, Int8(3), CGPoint(x: 1.0, y: 1.0), IntThing(a: 1))
    d.append(row: "Rake leaves", false, Int8(3), CGPoint(x: 2.0, y: 2.0), IntThing(a: 2))
    try d.writeSQL(connection:db, table: "newTable")
    // Test writing twice is OK
    d.append(row: "Watch TV", false, Int8(3), CGPoint(x: 2.0, y: 2.0), IntThing(a: 3))
    d.appendEmptyRow()
    try d.writeSQL(connection:db, table: "newTable")
    let newTable = try DataFrame(connection:db, table:"newTable")
    print(newTable)
    XCTAssertEqual(newTable.shape.rows, 4)
  }
  
  func testReadSQLTable() throws {
    var d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
      Column<Bool>(name:"done", capacity:0).eraseToAnyColumn(),
      Column<Int8>(name:"byte", capacity:0).eraseToAnyColumn(),
      Column<CGPoint>(name:"points", capacity:0).eraseToAnyColumn(),
      Column<IntThing>(name:"thing", capacity:0).eraseToAnyColumn()
    ])
    d.append(row: "Pick up drycleaning", false, Int8(3), CGPoint(x: 1.0, y: 1.0), IntThing(a: 1))
    d.append(row: "Rake leaves", false, Int8(3), CGPoint(x: 2.0, y: 2.0), IntThing(a: 2))
    try d.writeSQL(connection:db, table: "newTable")
    var d2 = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
      Column<Bool>(name:"done", capacity:0).eraseToAnyColumn(),
      Column<Int8>(name:"byte", capacity:0).eraseToAnyColumn(),
      Column<CGPoint>(name:"points", capacity:0).eraseToAnyColumn(),
      Column<IntThing>(name:"thing", capacity:0).eraseToAnyColumn()
    ])
    try d2.readSQL(connection:db, table:"newTable")
    XCTAssertEqual(d.shape.rows, d2.shape.rows)
    XCTAssertEqual(d.shape.columns, d2.shape.columns)
    XCTAssertEqual(d, d2)
    print(d2)
  }
  
  func testCasting() {
    
    let anyType: Any.Type = IntThing.self
    if case let sqliteDecodableType as SQLiteDecodable.Type = anyType {
      if let v = sqliteDecodableType.decodeSQL(sqliteValue: .int(17)) {
        print(v)
      }
    }
  }

}

/// A test SQLiteCodable type.
struct IntThing : SQLiteCodable {
  var a: Int

  init(a: Int) {
    self.a = a
  }
  
  init?(sqliteValue: SQLiteValue) {
    if case let .int(i) = sqliteValue {
      a = Int(i)
    } else {
      return nil
    }
  }
    
  var sqliteValue: SQLiteValue {
    .int(Int64(a))
  }
  
  
}
