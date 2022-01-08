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
    let newTable = try DataFrame(connection:db, table:"newTable")
    print(newTable)
    XCTAssertEqual(newTable.shape.rows, 2)
  }
  
  func testWriteSQLTableThatExistsWithFailPolicy() throws {
    let d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
    ])
    try d.writeSQL(connection:db, table: "newTable")
    XCTAssertThrowsError(try d.writeSQL(connection:db, table: "newTable", ifExists: .fail))
  }
  
  func testWriteSQLTableThatExistsWithDoNothingPolicy() throws {
    var d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
    ])
    d.append(row: "Rake leaves")
    try d.writeSQL(connection:db, table: "newTable")
    d.append(row: "Drink milk")
    try d.writeSQL(connection:db, table: "newTable", ifExists: .doNothing)
    let copy = try DataFrame(connection: db, table: "newTable")
    XCTAssertEqual(copy.shape.rows, 1)
  }
  
  func testWriteSQLTableThatExistsWithDoReplacePolicy() throws {
    var d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
    ])
    d.append(row: "Rake leaves")
    try d.writeSQL(connection:db, table: "newTable")
    d.append(row: "Drink milk")
    try d.writeSQL(connection:db, table: "newTable", ifExists: .replace)
    let copy = try DataFrame(connection: db, table: "newTable")
    XCTAssertEqual(copy.shape.rows, 2)
  }
  
  func testWriteSQLTableThatExistsWithAppendPolicy() throws {
    var d = DataFrame(columns: [
      Column<String>(name:"description", capacity:0).eraseToAnyColumn(),
    ])
    d.append(row: "Rake leaves")
    try d.writeSQL(connection:db, table: "newTable")
    // Test writing twice does nothing
    d.append(row: "Drink milk")
    try d.writeSQL(connection:db, table: "newTable", ifExists: .append)
    let copy = try DataFrame(connection: db, table: "newTable")
    XCTAssertEqual(copy.shape.rows, 3)
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
  
  func testCasting() throws {
    var statement: OpaquePointer!
    try check(sqlite3_prepare_v2(
      db, "select rowid from tasks where rowid = 2",-1,&statement,nil))
    XCTAssertEqual(try check(sqlite3_step(statement)), SQLITE_ROW)
    let anyType: Any.Type = IntThing.self
    if case let sqliteDecodableType as SQLiteDecodable.Type = anyType {
      if let v = sqliteDecodableType.decodeSQL(statement:statement, columnIndex: Int32(0)) {
        if case let thing as IntThing = v {
          XCTAssertEqual(thing.a, 2)
        } else {
          XCTFail("Got wrong type: \(type(of: v))")
        }
      } else {
        XCTFail("could not decode")
      }
    } else {
      XCTFail("could not cast")

    }
  }

}

/// A test SQLiteCodable type.
struct IntThing : SQLiteCodable {
  
  var a: Int

  init(a: Int) {
    self.a = a
  }
  
  init?(statement:OpaquePointer, columnIndex: Int32) {
    self.a = Int(sqlite3_column_int64(statement, columnIndex))
  }
  
  func encodeSQLiteValue() -> SQLiteValue {
    .int(Int64(a))
  }
  
}
