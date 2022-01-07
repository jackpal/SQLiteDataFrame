import SQLite3
import XCTest
@testable import SQLiteDataFrame

final class SQLiteCheckSQLiteTests: XCTestCase {
  
  func testCheckSQLiteError() {
    XCTAssertThrowsError(try checkSQLite(SQLITE_ERROR)) { error in
      let nsError = error as NSError
      XCTAssertEqual(nsError.domain, kSQLiteDataFrameDomain)
      XCTAssertEqual(nsError.code, Int(SQLITE_ERROR))
    }
  }
  
  func testCheckSQLiteNoError() throws {
    try checkSQLite(SQLITE_OK)
    try checkSQLite(SQLITE_ROW)
    try checkSQLite(SQLITE_DONE)
  }
}
