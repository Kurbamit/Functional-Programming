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

  describe "Lib2 - SQL Statement Parsing and Execution" $ do
    -- Testing parseStatement
    describe "parseStatement" $ do
      it "parses a valid SHOW TABLES statement" $ do
        parseStatement "show tables" `shouldBe` Right (SQLStatement ShowTables)

      it "parses a valid SHOW TABLE [name] statement" $ do
        parseStatement "show table employees" `shouldBe` Right (SQLStatement (ShowTableColumns "employees"))

      it "parses a valid SELECT statement" $ do
        parseStatement "select name from employees" `shouldSatisfy` isRight

      it "handles invalid input" $ do
        parseStatement "invalid statement" `shouldSatisfy` isLeft

  -- Testing executeStatement
  describe "executeStatement" $ do
    it "executes a SHOW TABLES statement" $ do
      executeStatement (SQLStatement ShowTables) `shouldBe` Right (DataFrame [Column "Tables" StringType] $ map (\(tableName, _) -> [StringValue tableName]) D.database)

    it "executes a SHOW TABLE [name] statement" $ do
      let table = snd D.tableEmployees
      let expectedColumns = case table of
            DataFrame columns _ -> columns
      executeStatement (SQLStatement (ShowTableColumns "employees")) `shouldBe` Right (DataFrame [Column "Columns" StringType] (columnNamesToRows expectedColumns))

    it "handles invalid statements" $ do
      executeStatement (SQLStatement ShowTables) `shouldSatisfy` isRight