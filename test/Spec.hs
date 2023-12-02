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
      Lib1.findTableByName Lib2.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName Lib2.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName Lib2.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
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
    it "parses WHERE statement with strings from a single table" $ do
      Lib2.parseStatement "SELECT name FROM employees WHERE name = Vi;" `shouldBe` Right (Select [ColumnWithAggregate "name" Nothing] ["employees"] [Limit "name" (StringValue "Vi")])
    it "parses WHERE statement with numbers from a single table" $ do
      Lib2.parseStatement "SELECT id FROM employees WHERE id = 1;" `shouldBe` Right (Select [ColumnWithAggregate "id" Nothing] ["employees"] [Limit "id" (IntegerValue 1)])
    it "parses WHERE statement with boolean values from a single table" $ do
      Lib2.parseStatement "SELECT flag, value FROM flags WHERE value = True;" `shouldBe` Right (Select [ColumnWithAggregate "flag" Nothing, ColumnWithAggregate "value" Nothing] ["flags"] [Limit "value" (BoolValue True)])
    it "parses a 'SELECT' statement with multiple columns and tables" $ do
      Lib2.parseStatement "SELECT id, name, flag, value FROM employees, flags;" `shouldBe` Right (Select [ColumnWithAggregate "id" Nothing, ColumnWithAggregate "name" Nothing, ColumnWithAggregate "flag" Nothing, ColumnWithAggregate "value" Nothing] ["employees", "flags"] [])
    it "parses a 'SELECT' statement with aggregate functions and multiple tables" $ do
      Lib2.parseStatement "SELECT max(id), sum(value) FROM employees, flags;" `shouldBe` Right (Select [ColumnWithAggregate "id" (Just Max), ColumnWithAggregate "value" (Just Sum)] ["employees", "flags"] [])
    it "parses WHERE statement with strings for multiple tables" $ do
      Lib2.parseStatement "SELECT name FROM employees, flags WHERE name = Vi;" `shouldBe` Right (Select [ColumnWithAggregate "name" Nothing] ["employees", "flags"] [Limit "name" (StringValue "Vi")])
    it "parses WHERE statement with numbers for multiple tables" $ do
      Lib2.parseStatement "SELECT id FROM employees, flags WHERE id = 1;" `shouldBe` Right (Select [ColumnWithAggregate "id" Nothing] ["employees", "flags"] [Limit "id" (IntegerValue 1)])
    it "parses WHERE statement with boolean values from multiple tables" $ do
      Lib2.parseStatement "SELECT flag, value FROM flags, employees WHERE value = True;" `shouldBe` Right (Select [ColumnWithAggregate "flag" Nothing, ColumnWithAggregate "value" Nothing] ["flags", "employees"] [Limit "value" (BoolValue True)])


  describe "Lib2.executeStatement" $ do
    it "executes a 'SHOW TABLES;' statement" $ do
      case Lib2.parseStatement "SHOW TABLES;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right showTablesTestResult
    it "fails to execute a 'SELECT' statement if (;) is missing" $ do
      case Lib2.parseStatement "SELECT id, name FROM employees" of 
        Left err -> err `shouldBe` "Empty or unfinished query"
        Right ps -> Lib2.executeStatement ps `shouldBe` Left missingSemicolonTestResult
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
    it "executes a 'SELECT' statement with multiple columns from multiple tables" $ do
      case Lib2.parseStatement "SELECT id, name, flag FROM employees, flags;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right selectMultipleTablesTestResult
    it "executes a 'SELECT' statement with multiple columns and tables" $ do
      case Lib2.parseStatement "SELECT id, name, flag, value FROM employees, flags;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right multipleColumnsAndTablesTestResult
    it "executes a 'SELECT' statement with aggregate functions and multiple tables" $ do
      case Lib2.parseStatement "SELECT max(id), sum(value) FROM employees, flags;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right aggregateFunctionsAndMultipleTablesTestResult

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

selectMultipleTablesTestResult :: DataFrame
selectMultipleTablesTestResult = DataFrame
  [Column "id" IntegerType, Column "name" StringType, Column "flag" StringType]
  [ [IntegerValue 1, StringValue "Vi", StringValue "a"],
    [IntegerValue 1, StringValue "Vi", StringValue "b"],
    [IntegerValue 1, StringValue "Vi", StringValue "b"],
    [IntegerValue 1, StringValue "Vi", StringValue "b"],
    [IntegerValue 2, StringValue "Ed", StringValue "a"],
    [IntegerValue 2, StringValue "Ed", StringValue "b"],
    [IntegerValue 2, StringValue "Ed", StringValue "b"],
    [IntegerValue 2, StringValue "Ed", StringValue "b"]
  ]

missingSemicolonTestResult :: String
missingSemicolonTestResult = "Error: Invalid statement. ';' is missing."

multipleColumnsAndTablesTestResult :: DataFrame
multipleColumnsAndTablesTestResult = DataFrame 
  [Column "id" IntegerType, Column "name" StringType, Column "flag" StringType, Column "value" BoolType] 
  [ [IntegerValue 1, StringValue "Vi", StringValue "a", BoolValue True], 
    [IntegerValue 1, StringValue "Vi", StringValue "b", BoolValue True], 
    [IntegerValue 1, StringValue "Vi", StringValue "b", NullValue], 
    [IntegerValue 1, StringValue "Vi", StringValue "b", BoolValue False], 
    [IntegerValue 2, StringValue "Ed", StringValue "a", BoolValue True], 
    [IntegerValue 2, StringValue "Ed", StringValue "b", BoolValue True], 
    [IntegerValue 2, StringValue "Ed", StringValue "b", NullValue], 
    [IntegerValue 2, StringValue "Ed", StringValue "b", BoolValue False]
  ]

aggregateFunctionsAndMultipleTablesTestResult :: DataFrame
aggregateFunctionsAndMultipleTablesTestResult = DataFrame
  [Column "id" IntegerType, Column "value" BoolType]
  [ [IntegerValue 2, NullValue] ]