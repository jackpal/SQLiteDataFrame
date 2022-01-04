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
      db, "select rowid, description, done, date from tasks order by rowid;",-1,&statement,nil))
    let dataFrame = try DataFrame(statement:statement)
    print(dataFrame)
    XCTAssertEqual(dataFrame.columns.count,4)
    XCTAssertEqual(dataFrame.rows.count,3)
  }
  
  func testDataFrameFilterColumns() throws {
    var statement: OpaquePointer!
    try check(sqlite3_prepare_v2(
      db, "select rowid, description, done from tasks order by rowid;",-1,&statement,nil))
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
    let statement = "select description, date from tasks order by description;"
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

}
