import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..))
import Lib1
import Lib2 
import Test.Hspec
import Lib3 (parseInsertStatement, parseDeleteStatement, parseUpdateStatement,executeSql, 
              ParsedInsertStatement(..), ParsedDeleteStatement(..), ParsedUpdateStatement(..), SetValue(..),
              insertStatement, deleteStatement, updateStatement)

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

  describe "Lib3.update functions" $ do
    it "parses full 'INSERT' statement" $ do
      Lib3.parseInsertStatement "INSERT INTO employees (id, name, surname) values (123, HaskellisJega, NOT);" `shouldBe` Right insertTestResult
    it "parses full 'INSERT' statement case-insensitively" $ do
      Lib3.parseInsertStatement "InSeRt InTo employees (id, name, surname) VaLuEs (123, HaskellisJega, NOT);" `shouldBe` Right insertTestResult
    it "parses 'INSERT' statement with missing column names" $ do
      Lib3.parseInsertStatement "INSERT INTO employees values (123, HaskellisJega, NOT);" `shouldBe` Right insertTestResult2
    it "parses 'INSERT' statement with missing column names case-insensitively" $ do
      Lib3.parseInsertStatement "InSeRt InTo employees VaLuEs (123, HaskellisJega, NOT);" `shouldBe` Right insertTestResult2
    it "parses 'INSERT' statement with missing column names and values" $ do
      Lib3.parseInsertStatement "INSERT INTO employees values;" `shouldSatisfy` isLeft
    it "parses 'INSERT' statement without every column name" $ do
      Lib3.parseInsertStatement "INSERT INTO employees (id) values (123);" `shouldBe` Right insertTestResult3
    it "parses 'INSERT' statement without every column name case-insensitively" $ do
      Lib3.parseInsertStatement "InSeRt InTo employees (id) VaLuEs (123);" `shouldBe` Right insertTestResult3
    it "parses 'INSERT' statement without every column name and value" $ do
      Lib3.parseInsertStatement "INSERT INTO employees (id) values;" `shouldSatisfy` isLeft
    it "do not parse 'INSERT' statement with missing 'INTO' keyword" $ do
      Lib3.parseInsertStatement "INSERT employees (id, name, surname) values (123, HaskellisJega, NOT);" `shouldSatisfy` isLeft
    it "do not parse 'INSERT' statement with missing 'INTO' keyword case-insensitively" $ do
      Lib3.parseInsertStatement "InSeRt employees (id, name, surname) VaLuEs (123, HaskellisJega, NOT);" `shouldSatisfy` isLeft
    it "do not parse 'INSERT' statement with missing 'VALUES' keyword" $ do
      Lib3.parseInsertStatement "INSERT INTO employees (id, name, surname) (123, HaskellisJega, NOT);" `shouldSatisfy` isLeft
    it "do not parse 'INSERT' statement with missing 'VALUES' keyword case-insensitively" $ do
      Lib3.parseInsertStatement "InSeRt InTo employees (id, name, surname) (123, HaskellisJega, NOT);" `shouldSatisfy` isLeft
    it "do not parse 'INSERT' statement without semi-colon" $ do
      Lib3.parseInsertStatement "INSERT INTO employees (id, name, surname) values (123, HaskellisJega, NOT)" `shouldSatisfy` isLeft
    it "parses 'DELETE' statement" $ do
      Lib3.parseDeleteStatement "DELETE FROM employees WHERE id = 1;" `shouldBe` Right deleteTestResult
    it "parses 'DELETE' statement case-insensitively" $ do
      Lib3.parseDeleteStatement "DeLeTe FrOm employees WhErE id = 1;" `shouldBe` Right deleteTestResult
    it "parses 'DELETE' statement with missing 'WHERE' keyword" $ do
      Lib3.parseDeleteStatement "DELETE FROM employees id = 1;" `shouldSatisfy` isLeft
    it "parses 'DELETE' statement with missing 'WHERE' keyword case-insensitively" $ do
      Lib3.parseDeleteStatement "DeLeTe FrOm employees id = 1;" `shouldSatisfy` isLeft
    it "parses 'DELETE' statement with missing 'WHERE' keyword and deletes everything in the table" $ do
      Lib3.parseDeleteStatement "DELETE FROM employees;" `shouldBe` Right deleteTestResult2
    it "parses 'DELETE' statement with missing 'WHERE' keyword and deletes everything in the table case-insensitively" $ do
      Lib3.parseDeleteStatement "DeLeTe FrOm employees;" `shouldBe` Right deleteTestResult2
    it "do not parse 'DELETE' statement without semi-colon" $ do
      Lib3.parseDeleteStatement "DELETE employees WHERE id = 1" `shouldSatisfy` isLeft
    it "do not parse 'DELETE' statement without 'WHERE' keyword" $ do
      Lib3.parseDeleteStatement "DELETE employees id = 1;" `shouldSatisfy` isLeft
    it "parses 'UPDATE' statement" $ do
      Lib3.parseUpdateStatement "UPDATE employees SET name = Ponas, surname = Krabas where id = 1;" `shouldBe` Right updateTestResult
    it "parses 'UPDATE' statement case-insensitively" $ do
      Lib3.parseUpdateStatement "UpDaTe employees SeT name = Ponas, surname = Krabas WhErE id = 1;" `shouldBe` Right updateTestResult
    it "parses 'UPDATE' statement with one argument" $ do
      Lib3.parseUpdateStatement "UPDATE employees SET id = 1 where id = 2;" `shouldBe` Right updateTestResult2
    it "parses 'UPDATE' statement with one argument case-insensitively" $ do
      Lib3.parseUpdateStatement "UpDaTe employees SeT id = 1 WhErE id = 2;" `shouldBe` Right updateTestResult2
    it "parses 'UPDATE' statement with missing 'WHERE' keyword" $ do
      Lib3.parseUpdateStatement "UPDATE employees SET name = Ponas;" `shouldBe` Right updateTestResult3
    it "do not parse 'UPDATE' statement without semi-colon" $ do
      Lib3.parseUpdateStatement "UPDATE employees SET name = Ponas" `shouldSatisfy` isLeft

  describe "Lib3.insertStatement" $ do
    it "inserts a new row into the table with valid input" $ do
      let table = "employees"
          columns = ["id", "name", "surname"]
          values = [IntegerValue 123, StringValue "HaskellisJega", StringValue "NOT"]
      case insertStatement table columns values of
          Left err -> expectationFailure $ "Unexpected error: " ++ err
          Right (DataFrame tableColumns tableRows) -> do
              length tableRows `shouldBe` 3
    it "fails to insert with invalid table name" $ do
      let table = "nonexistent_table"
          columns = ["id", "name", "surname"]
          values = [IntegerValue 123, StringValue "HaskellisJega", StringValue "NOT"]
      case insertStatement table columns values of
          Left err -> err `shouldBe` "Expected ';' in the query"
          Right _  -> expectationFailure "Expected an error, but the statement was executed successfully"
    it "fails to insert with mismatched column and value count" $ do
      let table = "employees"
          columns = ["id", "name", "surname"]
          values = [IntegerValue 123, StringValue "HaskellisJega"]
      case insertStatement table columns values of
          Left err -> err `shouldBe` "Incorrect specified number of values, 3 values expected"
          Right _  -> expectationFailure "Expected an error, but the statement was executed successfully"

  describe "Lib3.deleteStatement" $ do
    it "deletes rows with valid input" $ do
        let table = "employees"
            limit = Limit "name" (StringValue "Vi")
        case deleteStatement table limit of
            Left err -> expectationFailure $ "Unexpected error: " ++ err
            Right (DataFrame tableColumns tableRows) -> do
                length tableRows `shouldBe` 1
    it "fails to delete with invalid table name" $ do
        let table = "nonexistent_table"
            limit = Limit "name" (StringValue "Vi")
        case deleteStatement table limit of
            Left err -> err `shouldBe` "Expected ';' in the query"
            Right _  -> expectationFailure "Expected an error, but the statement was executed successfully"
    it "deletes rows with a valid limit" $ do
        let table = "employees"
            limit = Limit "name" (StringValue "Vi")
        case deleteStatement table limit of
            Left err -> expectationFailure $ "Unexpected error: " ++ err
            Right (DataFrame tableColumns tableRows) -> do
                length tableRows `shouldBe` 1
    it "fails to delete with an invalid limit" $ do
        let table = "employees"
            limit = Limit "invalid_column" (StringValue "value")
        case deleteStatement table limit of
            Left err -> err `shouldBe` "COLUMN 'invalid_column' does not exist in database"
            Right _  -> expectationFailure "Expected an error, but the statement was executed successfully"

  describe "Lib3.updateStatement" $ do
    it "updates rows with valid input" $ do
      let table = "employees"
          setValues = [SetValue "name" (StringValue "NewName")]
          limit = Limit "name" (StringValue "Vi")
      case updateStatement table setValues limit of
          Left err -> expectationFailure $ "Unexpected error: " ++ err
          Right (DataFrame tableColumns tableRows) -> do
              length tableRows `shouldBe` 2
    it "fails to update with invalid table name" $ do
        let table = "nonexistent_table"
            setValues = [SetValue "name" (StringValue "NewName")]
            limit = Limit "name" (StringValue "Vi")
        case updateStatement table setValues limit of
            Left err -> err `shouldBe` "Expected ';' in the query"
            Right _  -> expectationFailure "Expected an error, but the statement was executed successfully"
    it "updates rows with a valid limit" $ do
        let table = "employees"
            setValues = [SetValue "name" (StringValue "NewName")]
            limit = Limit "name" (StringValue "Vi")
        case updateStatement table setValues limit of
            Left err -> expectationFailure $ "Unexpected error: " ++ err
            Right (DataFrame tableColumns tableRows) -> do
                length tableRows `shouldBe` 2

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

insertTestResult :: ParsedInsertStatement
insertTestResult = Insert "employees" ["id", "name", "surname"] 
                  [IntegerValue 123, StringValue "HaskellisJega", StringValue "NOT"]

insertTestResult2 :: ParsedInsertStatement
insertTestResult2 = Insert "employees" [] 
                    [IntegerValue 123, StringValue "HaskellisJega", StringValue "NOT"]

insertTestResult3 :: ParsedInsertStatement
insertTestResult3 = Insert "employees" ["id"] 
                    [IntegerValue 123]

deleteTestResult :: ParsedDeleteStatement
deleteTestResult = Delete "employees" (Limit "id" (IntegerValue 1))

deleteTestResult2 :: ParsedDeleteStatement
deleteTestResult2 = Delete "employees" (Limit "" NullValue)

updateTestResult :: ParsedUpdateStatement
updateTestResult = Update "employees" [SetValue "name" (StringValue "Ponas"),SetValue "surname" (StringValue "Krabas")] (Limit "id" (IntegerValue 1))

updateTestResult2 :: ParsedUpdateStatement
updateTestResult2 = Update "employees" [SetValue "id" (IntegerValue 1)] (Limit "id" (IntegerValue 2))

updateTestResult3 :: ParsedUpdateStatement
updateTestResult3 = Update "employees" [SetValue "name" (StringValue "Ponas")] (Limit "" NullValue)