import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..))
import Lib1
import Lib2
import Test.Hspec
import Control.Monad (forM_)

data RunSqlTest = RunSqlTest
  { showTableDescription :: String
  , showTableSqlQuery :: String
  , expectedResult :: Either String [String]
  }

data SqlParsingTest = SqlParsingTest
  { selectDescription :: String
  , selectSqlQuery :: String
  , expectedStatement :: Either String ParsedStatement
  }

-- SHOW TABLE(S) test cases
runSqlTests :: [RunSqlTest]
runSqlTests =
  [ RunSqlTest "'SHOW TABLES' returns all tables names" "SHOW TABLES" (Right ["employees", "invalid1", "invalid2", "long_strings", "flags"])
  , RunSqlTest "SQL statements are case-insensitive" "ShOw TaBlEs" (Right ["employees", "invalid1", "invalid2", "long_strings", "flags"])
  , RunSqlTest "'SHOW TABLE' employees returns employee table column names" "SHOW TABLE employees" (Right ["id", "name", "surname"])
  , RunSqlTest "SQL statement'SHOW TABLE' employees is case-insensitive" "ShOw TaBlE employees" (Right ["id", "name", "surname"])
  , RunSqlTest "Table names are case-sensitive" "SHOW TABLE emploYEES" (Left "Table not found")
  , RunSqlTest "'ShOw TaBlE emploYEES' - table case-sensitive" "ShOw TaBlE emploYEES" (Left "Table not found")
  ]

-- SELECT test cases
sqlParsingTestCases :: [SqlParsingTest]
sqlParsingTestCases =
  [ SqlParsingTest "parses 'SELECT * FROM employees'" "SELECT * FROM employees" (Right (SQLStatement (Select "employees" ["*"])))
  , SqlParsingTest "parses 'SELECT name, surname FROM employees'" "SELECT name surname FROM employees" (Right (SQLStatement (Select "employees" ["name", "surname"])))
  ]

-- Case sensitivity test cases
sqlCaseSensitivityTestCases :: [SqlParsingTest]
sqlCaseSensitivityTestCases =
  [ SqlParsingTest "parses 'SELECT * FROM employees' (lowercase table name)" "SELECT * FROM employees" (Right (SQLStatement (Select "employees" ["*"])))
  , SqlParsingTest "parses 'SELECT * FROM Employees' (mixed case table name)" "SELECT * FROM Employees" (Right (SQLStatement (Select "Employees" ["*"])))
  , SqlParsingTest "parses 'SELECT * FROM EMPLOYEES' (uppercase table name)" "SELECT * FROM EMPLOYEES" (Right (SQLStatement (Select "EMPLOYEES" ["*"])))
  , SqlParsingTest "parses 'SELECT name, surname FROM employees' (lowercase table name)" "SELECT name surname FROM employees" (Right (SQLStatement (Select "employees" ["name", "surname"])))
  , SqlParsingTest "parses 'SELECT name, surname FROM Employees' (mixed case table name)" "SELECT name surname FROM Employees" (Right (SQLStatement (Select "Employees" ["name", "surname"])))
  , SqlParsingTest "parses 'SELECT name, surname FROM EMPLOYEES' (uppercase table name)" "SELECT name surname FROM EMPLOYEES" (Right (SQLStatement (Select "EMPLOYEES" ["name", "surname"])))

  -- Additional test cases for column name case-sensitivity
  , SqlParsingTest "parses 'SELECT NAME, SURNAME FROM employees' (uppercase column names)" "SELECT NAME SURNAME FROM employees" (Right (SQLStatement (Select "employees" ["NAME", "SURNAME"])))
  , SqlParsingTest "parses 'SELECT name, Surname FROM Employees' (mixed case column names with mixed case table name)" "SELECT name Surname FROM employees" (Right (SQLStatement (Select "employees" ["name", "Surname"])))
  ]

-- Not parsed statements
sqlNotParsedTestCases :: [SqlParsingTest]
sqlNotParsedTestCases =
  [ SqlParsingTest "random statement" "Blah blah" (Left "Not implemented: parseStatement")
  , SqlParsingTest "Similar statement 'SHOW'" "SHOW" (Left "Not implemented: parseStatement")
  ]

-------------------------------------------------------------------------------------------------------- 
main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()

-- SHOW TABLE(S) tests
  describe "SHOW TABLE(S) testing" $ do
    forM_ runSqlTests $ \test -> do
      it (showTableDescription test) $ do
        Lib2.runSql (showTableSqlQuery test) `shouldBe` expectedResult test

-- SELECT tests
  describe "SQL Parsing and Execution" $ do
    forM_ sqlParsingTestCases $ \test -> do
      it (selectDescription test) $ do
        parseStatement (selectSqlQuery test) `shouldBe` expectedStatement test
  
-- Case sensitivity tests
  describe "SQL Parsing and Execution (Table/Column Name Case-Sensitivity)" $ do
    forM_ sqlCaseSensitivityTestCases $ \test -> do
      it (selectDescription test) $ do
        parseStatement (selectSqlQuery test) `shouldBe` expectedStatement test

-- Not parsed statements tests
  describe "Not parsed statements" $ do
    forM_ sqlNotParsedTestCases $ \test -> do
      it (selectDescription test) $ do
        parseStatement (selectSqlQuery test) `shouldBe` expectedStatement test