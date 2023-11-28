import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..))
import Lib1
import Lib2 
import Test.Hspec
import Lib2 (ColumnWithAggregate(ColumnWithAggregate))

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

  describe "Lib2.parseStatement" $ do
    it "parses a 'SHOW TABLES;' statement" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right ShowTables
    it "parses a 'SHOW TABLES;' statement case insensitively" $ do
      Lib2.parseStatement "ShOw TaBlEs;" `shouldBe` Right ShowTables
    it "parser does not parse a 'SHOW TABLES' statement if (;) is missing" $ do
      Lib2.parseStatement "SHOW TABLES" `shouldSatisfy` isLeft
    it "parses a 'SHOW TABLE' statement" $ do
      Lib2.parseStatement "SHOW TABLE employees;" `shouldBe` Right (ShowTable "employees")
    it "parses 'SELECT *' and return all column names from specified table" $ do
      Lib2.parseStatement "SELECT * FROM employees;" `shouldBe` Right (Select [ColumnWithAggregate "*" Nothing] ["employees"] [])
    it "parses multiple columns from table" $ do
      Lib2.parseStatement "SELECT id, name FROM employees;" `shouldBe` Right (Select [ColumnWithAggregate "id" Nothing, ColumnWithAggregate "name" Nothing] ["employees"] [])
    it "parser does not parse invalid 'SELECT' statement" $ do
      Lib2.parseStatement "SELEC id FROM employees;" `shouldSatisfy` isLeft
    it "parser does not parse a 'SELECT' statement if (;) is missing" $ do
      Lib2.parseStatement "SELECT id, name FROM employees" `shouldSatisfy` isLeft
    it "parser does not parse a 'SELECT' statement if (,) is missing in column list" $ do
      Lib2.parseStatement "SELECT id name FROM employees;" `shouldSatisfy` isLeft
    it "parses 'MAX()' function" $ do
      Lib2.parseStatement "SELECT max(id) FROM employees;" `shouldBe` Right (Select [ColumnWithAggregate "id" (Just Max)] ["employees"] [])
    it "'MAX()' function is case-insensitive" $ do
      Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldBe` Right (Select [ColumnWithAggregate "id" (Just Max)] ["employees"] [])
    it "parses 'SUM()' function" $ do
      Lib2.parseStatement "SELECT sum(id) FROM employees;" `shouldBe` Right (Select [ColumnWithAggregate "id" (Just Sum)] ["employees"] [])
    it "'SUM()' function is case-insensitive" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` Right (Select [ColumnWithAggregate "id" (Just Sum)] ["employees"] [])
    it "parses WHERE statement with strings" $ do
      Lib2.parseStatement "SELECT name FROM employees WHERE name = Vi;" `shouldBe` Right (Select [ColumnWithAggregate "name" Nothing] ["employees"] [Limit "name" (StringValue "Vi")])
    it "parses WHERE statement with numbers" $ do
      Lib2.parseStatement "SELECT id FROM employees WHERE id = 1;" `shouldBe` Right (Select [ColumnWithAggregate "id" Nothing] ["employees"] [Limit "id" (IntegerValue 1)])
    it "parses WHERE statement with boolean values" $ do
      Lib2.parseStatement "SELECT flag, value FROM flags WHERE value = True;" `shouldBe` Right (Select [ColumnWithAggregate "flag" Nothing, ColumnWithAggregate "value" Nothing] ["flags"] [Limit "value" (BoolValue True)])

  describe "Lib2.executeStatement" $ do
    it "executes a 'SHOW TABLES;' statement" $ do
      case Lib2.parseStatement "SHOW TABLES;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTablesTestResult
    it "executes a 'SHOW TABLES;' case insensitively" $ do
      case Lib2.parseStatement "SHOW TABLES;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTablesTestResult
    it "executes a 'SHOW TABLE employees;' statement" $ do
      case Lib2.parseStatement "SHOW TABLE employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTableTestResult
    it "executes a 'SHOW TABLE employees;' statement case insensitively" $ do
      case Lib2.parseStatement "ShOw TaBlE employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTableTestResult
    it "does not execute a 'SHOW TABLE' statement with case-mismatching table name" $ do
      case Lib2.parseStatement "SHOW TABLE EmPlOyEeS;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes a 'SELECT' statement with multiple columns" $ do
      case Lib2.parseStatement "SELECT id, name FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right selectTestResult
    it "does not execute a 'SELECT' statement with case-mismatching table name" $ do
      case Lib2.parseStatement "SELECT id FROM EmployeeS;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "does not execute a 'SELECT' statement with non-existing column names" $ do
      case Lib2.parseStatement "SELECT idd FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "does not execute a 'SELECT' statement with case-mismatching column names" $ do
      case Lib2.parseStatement "SELECT Id FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes a 'MAX' function with integer" $ do
      case Lib2.parseStatement "SELECT MAX(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right maxTestResult1
    it "executes a 'MAX' function with string" $ do
      case Lib2.parseStatement "SELECT MAX(name) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right maxTestResult2
    it "does not execute a 'MAX' function with case-mismatching column name" $ do
      case Lib2.parseStatement "SELECT MAX(Surname) FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes a 'SUM' function" $ do
      case Lib2.parseStatement "SELECT SUM(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right sumTestResult
    it "does not execute a 'SUM' function with case-mismatching column name" $ do
      case Lib2.parseStatement "SELECT SUM(Id) FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes 'WHERE' statement" $ do
      case Lib2.parseStatement "SELECT id, name FROM employees WHERE id = 1;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right whereTestResult1
    it "does not execute 'WHERE' statement with case-mismatching column name" $ do
      case Lib2.parseStatement "SELECT id, name FROM employees WHERE Id = 1;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes 'WHERE OR' statement" $ do
      case Lib2.parseStatement "SELECT id, surname FROM employees WHERE id = 1 OR surname = Dl;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right whereTestResult2

showTablesTestResult :: DataFrame
showTablesTestResult = DataFrame
  [Column "Tables" StringType]
  [ 
    [StringValue "employees"],
    [StringValue "invalid1"],
    [StringValue "invalid2"],
    [StringValue "long_strings"],
    [StringValue "flags"]
  ]

showTableTestResult :: DataFrame
showTableTestResult = DataFrame
  [Column "Columns" StringType]
  [
    [StringValue "id"],
    [StringValue "name"],
    [StringValue "surname"]
  ]

selectTestResult :: DataFrame
selectTestResult = DataFrame
  [Column "id" IntegerType, Column "name" StringType]
  [ [IntegerValue 1, StringValue "Vi"],
    [IntegerValue 2, StringValue "Ed"]
  ]

maxTestResult1 :: DataFrame
maxTestResult1 = DataFrame
  [Column "id" IntegerType] 
  [[IntegerValue 2]]

maxTestResult2 :: DataFrame
maxTestResult2 = DataFrame [Column "name" StringType] [[StringValue "Vi"]]

sumTestResult :: DataFrame
sumTestResult = DataFrame [Column "id" IntegerType] [[IntegerValue 3]]

whereTestResult1 :: DataFrame
whereTestResult1 = DataFrame
  [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Vi"]]

whereTestResult2 :: DataFrame
whereTestResult2 = DataFrame [Column "id" IntegerType, Column "surname" StringType] 
                             [  [IntegerValue 1, StringValue "Po"], 
                                [IntegerValue 2, StringValue "Dl"] 
                             ]

type ErrorMessage = String

-- parseStatementTestCase :: String -> Either ErrorMessage ParsedStatement
-- parseStatementTestCase "SHOW TABLES" = Right (SQLStatement ShowTables)
-- parseStatementTestCase "SHOW TABLE" = Right (SQLStatement (ShowTableColumns "employees"))
-- parseStatementTestCase "SELECT *" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "*" Nothing] []))
-- parseStatementTestCase "SELECT id, name" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" Nothing, ColumnWithAggregate "name" Nothing] []))
-- parseStatementTestCase "AGGREGATE MAX()" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" (Just Max)] []))
-- parseStatementTestCase "AGGREGATE MAX(String)" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "name" (Just Max)] []))
-- parseStatementTestCase "AGGREGATE SUM()" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" (Just Sum)] []))
-- parseStatementTestCase "BASIC WHERE STRING" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "name" Nothing] [Limit "name" (StringValue "Vi")]))
-- parseStatementTestCase "BASIC WHERE INT" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" Nothing] [Limit "id" (IntegerValue 1)]))
-- parseStatementTestCase "BASIC WHERE INT 2" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" Nothing, ColumnWithAggregate "name" Nothing] [Limit "id" (IntegerValue 1)]))
-- parseStatementTestCase "WHERE TRUE" = Right (SQLStatement (Select "flags" [ColumnWithAggregate "flag" Nothing, ColumnWithAggregate "value" Nothing] [Limit "value" (BoolValue True)]))
-- parseStatementTestCase "WHERE OR" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" Nothing, ColumnWithAggregate "name" Nothing] [Limit "id" (IntegerValue 1), Limit "name" (StringValue "Ed")]))
-- parseStatementTestCase "WHERE OR 2" = Right (SQLStatement (Select "employees" [ColumnWithAggregate "id" Nothing, ColumnWithAggregate "surname" Nothing] [Limit "id" (IntegerValue 1), Limit "surname" (StringValue "Dl")]))
-- parseStatementTestCase _ = Left "Unsupported statement"