import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..))
import Lib1
import Lib2 
import Test.Hspec

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
      Lib2.parseStatement "SHOW TABLES;" `shouldSatisfy` isRight
    it "parses a 'SHOW TABLES;' statement case insensitively" $ do
      Lib2.parseStatement "ShOw TaBlEs;" `shouldSatisfy` isRight
    it "parses a 'SHOW TABLE' statement" $ do
      Lib2.parseStatement "SHOW TABLE employees;" `shouldSatisfy` isRight
    it "parses everything from table" $ do
      Lib2.parseStatement "SELECT * FROM employees;" `shouldSatisfy` isRight
    it "parses multiple columns from table" $ do
      Lib2.parseStatement "SELECT id, name FROM employees;" `shouldSatisfy` isRight
    it "parser do not parse invalid 'SELECT' statement" $ do
      Lib2.parseStatement "SELEC id FROM employees;" `shouldSatisfy` isLeft
    it "parses max function" $ do
      Lib2.parseStatement "SELECT max(id) FROM employees;" `shouldSatisfy` isRight
    it "'MAX()' function is case-insensitive" $ do
      Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldSatisfy` isRight
    it "parses sum function" $ do
      Lib2.parseStatement "SELECT sum(id) FROM employees;" `shouldSatisfy` isRight
    it "'SUM()' function is case-insensitive" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldSatisfy` isRight
    it "parses WHERE statement with strings" $ do
      Lib2.parseStatement "SELECT name FROM employees WHERE name = 'Vi';" `shouldSatisfy` isRight
    it "parses WHERE statement with numbers" $ do
      Lib2.parseStatement "SELECT id FROM employees WHERE id = 1;" `shouldSatisfy` isRight
    it "parses WHERE statement with OR" $ do
      Lib2.parseStatement "SELECT id FROM employees WHERE id > 0 OR id < 10;" `shouldSatisfy` isRight
    it "parses a where or function with strings, combined with sum" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees WHERE name <= 'E' OR surname <= 'E';" `shouldSatisfy` isRight

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
    it "does not execute a 'SHOW TABLE' statement with a case insensitive name" $ do
      case Lib2.parseStatement "SHOW TABLE EmPlOyEeS;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes a 'SELECT' statement with columns" $ do
      case Lib2.parseStatement "SELECT id, surname FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right selectTestResult
    it "does not execute a 'SELECT' statement with wrong column names" $ do
      case Lib2.parseStatement "SELECT idd FROM employees;" of
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
    it "executes a 'SUM' function" $ do
      case Lib2.parseStatement "SELECT SUM(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right sumTestResult
    it "executes 'WHERE' statement" $ do
      case Lib2.parseStatement "SELECT id, name FROM employees WHERE id = 1;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right whereTestResult1
    it "executes 'WHERE OR' statement" $ do
      case Lib2.parseStatement "SELECT id FROM employees WHERE id = 1 OR id = 2;" of 
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
  [Column "id" IntegerType, Column "surname" StringType]
  [ [IntegerValue 1, StringValue "Po"],
    [IntegerValue 2, StringValue "Dl"]
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
whereTestResult2 = DataFrame [Column "id" IntegerType] [[IntegerValue 1], [IntegerValue 2]]