import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..))
import Lib1
import Lib2
import Test.Hspec
import Control.Monad (forM_)

data RunSqlTest = RunSqlTest
  { description :: String
  , sqlQuery :: String
  , expectedResult :: Either String [String]
  }

runSqlTests :: [RunSqlTest]
runSqlTests =
  [ RunSqlTest "'SHOW TABLES' returns all tables names" "SHOW TABLES" (Right ["employees", "invalid1", "invalid2", "long_strings", "flags"])
  , RunSqlTest "SQL statements are case-insensitive" "ShOw TaBlEs" (Right ["employees", "invalid1", "invalid2", "long_strings", "flags"])
  , RunSqlTest "'SHOW TABLE' employees returns employee table column names" "SHOW TABLE employees" (Right ["id", "name", "surname"])
  , RunSqlTest "SQL statement'SHOW TABLE' employees is case-insensitive" "ShOw TaBlE employees" (Right ["id", "name", "surname"])
  , RunSqlTest "Table names are case-sensitive" "SHOW TABLE emploYEES" (Left "Table not found")
  , RunSqlTest "'ShOw TaBlE emploYEES' - table case-sensitive" "ShOw TaBlE emploYEES" (Left "Table not found")
  ]

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

  describe "SHOW TABLE(S) testing" $ do
    forM_ runSqlTests $ \test -> do
      it (description test) $ do
        Lib2.runSql (sqlQuery test) `shouldBe` expectedResult test

  describe "SQL Parsing and Execution" $ do
    it "parses 'SELECT * FROM employees'" $ do
      let sqlStatement = "SELECT * FROM employees"
      let expectedParsedStatement = Right (SQLStatement (Select "employees" ["*"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement

    it "parses 'SELECT name, surname FROM employees'" $ do
      let sqlStatement = "SELECT name surname FROM employees"
      let expectedParsedStatement = Right (SQLStatement (Select "employees" ["name", "surname"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement
  
  describe "SQL Parsing and Execution (Table Name Case-Sensitivity)" $ do
    it "parses 'SELECT * FROM employees' (lowercase table name)" $ do
      let sqlStatement = "SELECT * FROM employees"
      let expectedParsedStatement = Right (SQLStatement (Select "employees" ["*"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement

    it "parses 'SELECT * FROM Employees' (mixed case table name)" $ do
      let sqlStatement = "SELECT * FROM Employees"
      let expectedParsedStatement = Right (SQLStatement (Select "Employees" ["*"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement

    it "parses 'SELECT * FROM EMPLOYEES' (uppercase table name)" $ do
      let sqlStatement = "SELECT * FROM EMPLOYEES"
      let expectedParsedStatement = Right (SQLStatement (Select "EMPLOYEES" ["*"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement

    it "parses 'SELECT name, surname FROM employees' (lowercase table name)" $ do
      let sqlStatement = "SELECT name surname FROM employees"
      let expectedParsedStatement = Right (SQLStatement (Select "employees" ["name", "surname"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement

    it "parses 'SELECT name, surname FROM Employees' (mixed case table name)" $ do
      let sqlStatement = "SELECT name surname FROM Employees"
      let expectedParsedStatement = Right (SQLStatement (Select "Employees" ["name", "surname"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement

    it "parses 'SELECT name, surname FROM EMPLOYEES' (uppercase table name)" $ do
      let sqlStatement = "SELECT name surname FROM EMPLOYEES"
      let expectedParsedStatement = Right (SQLStatement (Select "EMPLOYEES" ["name", "surname"]))
      parseStatement sqlStatement `shouldBe` expectedParsedStatement
  
  describe "Not parsed statements" $ do
    it "random statement" $ do
      let sqlStatement = "Blah blah"
      let expectedParsedStatement = Left "Not implemented: parseStatement"
      parseStatement sqlStatement `shouldBe` expectedParsedStatement
    
    it "Similar statement 'SHOW'" $ do
      let sqlStatement = "SHOW"
      let expectedParsedStatement = Left "Not implemented: parseStatement"
      parseStatement sqlStatement `shouldBe` expectedParsedStatement