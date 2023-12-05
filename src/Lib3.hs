{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    ParsedInsertStatement(..),
    ParsedDeleteStatement(..),
    ParsedUpdateStatement(..),
    SetValue(..),
    saveDatabaseToJSON,
    parseInsertStatement,
    parseDeleteStatement,
    parseUpdateStatement,
    insertStatement,
    deleteStatement,
    updateStatement
  )
where

import Control.Monad.Free (Free (..), liftF)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Aeson as Aeson
import GHC.Generics (Generic)
import DataFrame (DataFrame(..), Column(..), Row(..), ColumnType(..), Value(..))
import Data.Time ( UTCTime )
import Lib2
import Data.Functor.Classes (readData)
import Data.Maybe (fromMaybe)
import Data.List (intersperse)
import GHC.OldList (elemIndex)
import Data.Data (Data)
import Data.Aeson.Encoding (value, string)
import Data.List (sortBy)
import Data.Functor.Identity (Identity)
import Data.List (find)
import GHC.IO (unsafePerformIO)
import Data.Char (toLower)
import Control.Monad.Trans.Error (Error)
import qualified Data.ByteString.Char8 as BC
import Data.ByteString (ByteString)

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = GetTime (UTCTime -> next)
  | SaveFile TableName FileContent (() -> next)
  | LoadFile (FileContent -> next)
  deriving Functor

type ColumnName = String

data ParsedInsertStatement
  = Insert TableName [ColumnName] [DataFrame.Value]
      deriving (Show, Eq)

data ParsedDeleteStatement
  = Delete TableName Limit
    deriving (Show, Eq)

data ParsedUpdateStatement
  = Update TableName [SetValue] Limit
  deriving (Show, Eq)

data SetValue = SetValue String DataFrame.Value
  deriving (Show, Eq)

data Statements
  = ParseStatement
  | ParseInsertStatement
  | ParseDeleteStatement
  | ParseUpdateStatement
  deriving Show

type Execution = Free ExecutionAlgebra

saveFile :: TableName -> FileContent -> Execution ()
saveFile name content = liftF $ SaveFile name content id

loadFile :: Execution FileContent
loadFile = liftF $ LoadFile id

getDatabaseContent :: Execution (Either ErrorMessage Database)
getDatabaseContent = do
  file <- loadFile
  case decode (BLC.pack file) of 
    Nothing -> return $ Left $ "Failed to deserialize 'database.json'"
    Just db -> Pure $ Right db

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

setValueParser :: Parser SetValue
setValueParser = do
  _ <- optional whiteSpaceParser
  columnName <- alphanumericParser
  _ <- optional whiteSpaceParser
  _ <- stringParser "="
  _ <- optional whiteSpaceParser
  extractedValue <- alphanumericParser
  pure (SetValue columnName (getValueType extractedValue))

setSectionParser :: Parser [SetValue]
setSectionParser = separate setValueParser (stringParser ",")

updateParser :: Parser ParsedUpdateStatement
updateParser = do
  _ <- stringParser "update"
  _ <- whiteSpaceParser
  table <- alphanumericParser
  _ <- whiteSpaceParser
  _ <- stringParser "set"
  _ <- whiteSpaceParser
  setValues <- setSectionParser
  limit <- optional $ do
    _ <- whiteSpaceParser
    _ <- stringParser "where"
    _ <- whiteSpaceParser
    limit <- whereConditionParser
    pure limit
  _ <- stringParser ";"
  pure (Update table setValues (fromMaybe (Limit "" NullValue) limit))

whereConditionParser :: Parser Limit
whereConditionParser = do
  columnName <- alphanumericParser
  _ <- optional whiteSpaceParser
  _ <- stringParser "="
  _ <- optional whiteSpaceParser
  value <- alphanumericParser
  pure (Limit columnName (getValueType value))

deleteParser :: Parser ParsedDeleteStatement
deleteParser = do
  _ <- stringParser "delete"
  _ <- whiteSpaceParser
  _ <- stringParser "from"
  _ <- whiteSpaceParser
  table <- alphanumericParser
  limit <- optional $ do
    _ <- whiteSpaceParser
    _ <- stringParser "where"
    _ <- whiteSpaceParser
    limit <- whereConditionParser
    pure limit
  _ <- stringParser ";"
  pure (Delete table (fromMaybe (Limit "" NullValue) limit))

columnNameParser :: Parser ColumnName
columnNameParser = do
  _ <- optional whiteSpaceParser
  column <- alphanumericParser
  pure column

columnNameSectionParser :: Parser [ColumnName]
columnNameSectionParser = separate columnNameParser (stringParser ",")

valueParser :: Parser String
valueParser = do
  _ <- optional whiteSpaceParser
  value <- alphanumericParser
  pure value

valueSectionParser :: Parser [DataFrame.Value]
valueSectionParser = separate valueParser (stringParser ",") >>= mapM (return . getValueType)

insertParser :: Parser ParsedInsertStatement
insertParser = do
  _ <- stringParser "insert"
  _ <- whiteSpaceParser
  _ <- stringParser "into"
  _ <- whiteSpaceParser
  table <- alphanumericParser
  _ <- whiteSpaceParser
  columns <- optional $ do
    _ <- stringParser "("
    columns <- columnNameSectionParser
    _ <- stringParser ")"
    _ <- whiteSpaceParser
    pure columns
  _ <- stringParser "values"
  _ <- whiteSpaceParser
  _ <- stringParser "("
  values <- valueSectionParser
  _ <- stringParser ")"
  _ <- stringParser ";"
  pure (Insert table (fromMaybe [] columns) values)


parseUpdateStatement :: String -> Either ErrorMessage ParsedUpdateStatement
parseUpdateStatement input = case runParser parser input of
  Left errorMessage -> Left errorMessage
  Right (input, remainder) ->
    case input of
      Update _ _ _ -> case runParser (optional whiteSpaceParser) remainder of
        Left errorMessage -> Left errorMessage
        Right _ -> Right input
  where
    parser :: Parser ParsedUpdateStatement
    parser = updateParser

parseDeleteStatement :: String -> Either ErrorMessage ParsedDeleteStatement
parseDeleteStatement input = case runParser parser input of
  Left errorMessage -> Left errorMessage
  Right (input, remainder) ->
    case input of
      Delete _ _ -> case runParser (optional whiteSpaceParser) remainder of
        Left errorMessage -> Left errorMessage
        Right _ -> Right input
  where
    parser :: Parser ParsedDeleteStatement
    parser = deleteParser

parseInsertStatement :: String -> Either ErrorMessage ParsedInsertStatement
parseInsertStatement input = case runParser parser input of
  Left errorMessage -> Left errorMessage
  Right (input, remainder) ->
    case input of
      Insert _ _ _ -> case runParser (optional whiteSpaceParser) remainder of
        Left errorMessage -> Left errorMessage
        Right _ -> Right input
  where
    parser :: Parser ParsedInsertStatement
    parser = insertParser


convertColumnsToString :: [Column] -> [ColumnName]
convertColumnsToString columns =
  map (\(Column name _) -> name) columns

getColumnsWithIndexes :: [ColumnName] -> [ColumnName] -> [(Int, ColumnName)]
getColumnsWithIndexes selectedColumns tableColumns =
  let getIndex column = case elemIndex column tableColumns of
        Just index -> index
        Nothing -> error ("Column not found: " ++ column)
  in [(index, column) | column <- selectedColumns, let index = getIndex column]

zipLists :: [DataFrame.Value] -> [(Int, ColumnName)] -> [(DataFrame.Value, Int, ColumnName)]
zipLists values indexes = zipWith (\v (i, c) -> (v, i, c)) values indexes

sortList :: [(DataFrame.Value, Int, ColumnName)] -> [(DataFrame.Value, Int, ColumnName)]
sortList list =
  sortBy (\(_, index1, _) (_, index2, _) -> compare index1 index2) list

formNewValueList :: Int -> [(DataFrame.Value, Int, ColumnName)] -> [DataFrame.Value]
formNewValueList lengthOfNewList sortedList =
  map (\index -> getValueForIndex index sortedList) [0..lengthOfNewList - 1]

-- Function to get the value for a specific index
getValueForIndex :: Int -> [(DataFrame.Value, Int, ColumnName)] -> DataFrame.Value
getValueForIndex index sortedList =
  case find (\(_, i, _) -> i == index) sortedList of
    Just (value, _, _) -> value
    Nothing -> DataFrame.NullValue

addNewRow :: Row -> [Row] -> [Row]
addNewRow newRow list = list ++ [newRow]

checkIfColumnSectionIsSkipped :: [ColumnName] -> Bool
checkIfColumnSectionIsSkipped = null

findDataFrame :: TableName -> Either ErrorMessage DataFrame
findDataFrame table = 
  case find (\(name, _) -> name == table) database of
    Just (_, dataframe) -> Right dataframe
    Nothing -> Left $ "No TABLE with name '" ++ table ++ "' was found"

validateInsertStatement :: TableName -> [ColumnName] -> [DataFrame.Value] -> Either ErrorMessage Bool
validateInsertStatement table columns values =
  case findDataFrame table of
    Right (DataFrame tableColumns _) -> 
      case validateDatabaseColumns [table] columns of
        True -> 
          if null columns
            then
              if length values == length tableColumns
                then Right True
                else Left $ "Incorrect specified number of values, " ++ show (length tableColumns) ++ " values expected"
            else 
              if length columns == length values
                then Right True
                else Left $ "Incorrect specified number of values, " ++ show (length columns) ++ " values expected"
        False -> Left $ "Such COLUMNS do not exist in database"
    Left errorMessage -> Left errorMessage 

insertStatement :: TableName -> [ColumnName] -> [DataFrame.Value] -> Either ErrorMessage DataFrame
insertStatement table columns values = 
  case parseStatement ("select * from " ++ table ++ ";") of
    Left errorMessage -> Left errorMessage
    Right parsedStatement -> 
      case validateInsertStatement table columns values of
        Right _ ->
          case executeStatement parsedStatement of
            Left errorMessage -> Left errorMessage
            Right (DataFrame tableColumns tableRows) -> 
              case checkIfColumnSectionIsSkipped columns of
                True -> 
                  Right (DataFrame tableColumns (addNewRow values tableRows))
                False ->
                  Right (DataFrame tableColumns (addNewRow (formNewValueList (length tableColumns) (sortList (zipLists values (getColumnsWithIndexes columns (convertColumnsToString tableColumns))))) tableRows))
        Left errorMessage -> Left errorMessage

checkIfLimitSectionIsSkipped :: Limit -> Bool
checkIfLimitSectionIsSkipped (Limit name value) =
  if (name == "" && value == NullValue)
    then True
    else False

getNameFromLimit :: Limit -> String
getNameFromLimit (Limit name value) = name

findColumnIndexInList :: [Column] -> String -> Int
findColumnIndexInList columns columnName =
  case [index | (Column name _, index) <- zip columns [0..], name == columnName] of
    [index] -> index

getValueAtIndex :: Int -> Row -> DataFrame.Value
getValueAtIndex index values = values !! index

deleteRows :: DataFrame -> Limit -> Either ErrorMessage DataFrame
deleteRows (DataFrame tableColumns tableRows) (Limit columnName value) =
  case findColumnIndexInList tableColumns columnName of
    columnIndex ->
      if all (\row -> getValueAtIndex columnIndex row /= value) tableRows
        then Left $ "Value '" ++ (show value) ++ "' was not found in column '" ++ columnName ++ "'"
        else Right $ DataFrame tableColumns (filter (\row -> getValueAtIndex columnIndex row /= value) tableRows)

validateDeleteStatement :: TableName -> Limit -> Either ErrorMessage Bool
validateDeleteStatement table limit =
  case findDataFrame table of
    Right (DataFrame tableColumns _) ->
      case checkIfLimitSectionIsSkipped limit of
        True -> Right True
        False ->
          case validateDatabaseColumns [table] [(getNameFromLimit limit)] of
            True -> Right True
            False -> Left $ "COLUMN '" ++ (getNameFromLimit limit) ++ "' does not exist in database"
    Left errorMessage -> Left errorMessage

deleteStatement :: TableName -> Limit -> Either ErrorMessage DataFrame
deleteStatement table limit =
  case parseStatement ("select * from " ++ table ++ ";") of
    Left errorMessage -> Left errorMessage
    Right parsedStatement ->
      case validateDeleteStatement table limit of 
        Right _ ->
          case executeStatement parsedStatement of
            Left errorMessage -> Left errorMessage
            Right (DataFrame tableColumns tableRows) ->
              case checkIfLimitSectionIsSkipped limit of
                True ->
                  Right (DataFrame tableColumns []) 
                False -> 
                  case (deleteRows (DataFrame tableColumns tableRows) limit) of
                    Left errorMessage -> Left errorMessage
                    Right result -> Right result
        Left errorMessage -> Left errorMessage

getNamesFromSetValues :: [SetValue] -> [String]
getNamesFromSetValues setValues = map (\(SetValue name _) -> name) setValues

updateColumn :: DataFrame -> [SetValue] -> Either ErrorMessage DataFrame
updateColumn dataframe setValues =
  foldl (\eitherDF setValue -> eitherDF >>= \df -> updateEachColumn df setValue) (Right dataframe) setValues

updateEachColumn :: DataFrame -> SetValue -> Either ErrorMessage DataFrame
updateEachColumn (DataFrame tableColumns tableRows) setValue =
  case updateEachRowBasedOnColumn (DataFrame tableColumns tableRows) setValue of
    Right result -> Right result
    Left errorMessage -> Left errorMessage

updateEachRowBasedOnColumn :: DataFrame -> SetValue -> Either ErrorMessage DataFrame
updateEachRowBasedOnColumn (DataFrame tableColumns tableRows) (SetValue columnName value) =
  case findColumnIndexInList tableColumns columnName of
    columnIndex -> 
      (Right (DataFrame tableColumns (map (\row -> (createNewRow row columnIndex value)) tableRows)))

createNewRow :: Row -> Int -> DataFrame.Value -> Row
createNewRow values index newValue =
  take index values ++ [newValue] ++ drop (index + 1) values

updateEachColumnBasedOnCondition :: DataFrame -> [SetValue] -> Limit -> Either ErrorMessage DataFrame
updateEachColumnBasedOnCondition (DataFrame tableColumns tableRows) setValues (Limit columnName' value') =
  case findColumnIndexInList tableColumns columnName' of
    columnIndex ->
      case checkIfValueExistsInRows tableRows columnIndex value' of
        Left errorMessage -> Left errorMessage
        Right indexes -> foldl (\eitherDF index -> eitherDF >>= \df -> updateRowBasedOnCondition index df setValues) (Right (DataFrame tableColumns tableRows)) indexes

mapIndexed :: (Int -> a -> b) -> [a] -> [b]
mapIndexed f xs = zipWith f [0..] xs

updateRowBasedOnCondition :: Int -> DataFrame -> [SetValue] -> Either ErrorMessage DataFrame
updateRowBasedOnCondition rowToModifyIndex (DataFrame tableColumns tableRows) setValues =
  let updatedRows = foldl (\rows (SetValue columnName newValue) ->
        case findColumnIndexInList tableColumns columnName of
          columnIndex ->
            mapIndexed (\i row ->
              if i == rowToModifyIndex
                then createNewRow row columnIndex newValue
                else row
            ) rows
        ) tableRows setValues
  in Right $ DataFrame tableColumns updatedRows

checkIfValueExistsInRows :: [Row] -> Int -> DataFrame.Value -> Either ErrorMessage [Int]
checkIfValueExistsInRows rows index valueToFind =
  let matchingIndexes = [i | (i, row) <- zip [0..] rows, getValueAtIndex index row == valueToFind]
  in if null matchingIndexes
      then Left $ "Value '" ++ show valueToFind ++ "' not found in the specified column"
      else Right matchingIndexes

validateUpdateStatement :: TableName -> [SetValue] -> Limit ->  Either ErrorMessage Bool
validateUpdateStatement table setValues limit = 
  case findDataFrame table of
    Right (DataFrame tableColumns _) ->
      case validateDatabaseColumns [table] (getNamesFromSetValues setValues) of
        True -> 
          case checkIfLimitSectionIsSkipped limit of
            True -> Right True
            False ->
              case validateDatabaseColumns [table] [(getNameFromLimit limit)] of
                True -> Right True
                False -> Left $ "Such COLUMNS do not exist in database (WHERE section)"
        False -> Left $ "Such COLUMNS do not exist in database (SET section)"
    Left errorMessage -> Left errorMessage

updateStatement :: TableName -> [SetValue] -> Limit -> Either ErrorMessage DataFrame
updateStatement table setValues limit =
  case parseStatement ("select * from " ++ table ++ ";") of
    Left errorMessage -> Left errorMessage
    Right parsedStatement ->
      case validateUpdateStatement table setValues limit of
        Right _ -> 
          case executeStatement parsedStatement of
            Left errorMessage -> Left errorMessage
            Right (DataFrame tableColumns tableRows) ->
              case checkIfLimitSectionIsSkipped limit of
                True -> 
                  case updateColumn (DataFrame tableColumns tableRows) setValues of
                    Right result -> Right result
                    Left errorMessage -> Left errorMessage
                False ->
                  case updateEachColumnBasedOnCondition (DataFrame tableColumns tableRows) setValues limit of
                    Right result -> Right result
                    Left errorMessage -> Left errorMessage
        Left errorMessage -> Left errorMessage


updateDatabase :: TableName -> DataFrame -> Database
updateDatabase tableName newDataFrame = 
  (tableName, newDataFrame) : filter (\(name, _) -> name /= tableName) database

-- saveDatabaseToJSON :: Database -> BLC.ByteString
-- saveDatabaseToJSON updatedDatabase = do
--   let filePath = "src/db/database.json"
--       contentByteString = encode updatedDatabase
--   -- BLC.writeFile filePath contentByteString
--   return contentByteString

saveDatabaseToJSON :: Database -> FileContent
saveDatabaseToJSON updatedDatabase =
  BLC.unpack (encode updatedDatabase)

updateFile :: TableName -> DataFrame -> Execution ()
updateFile name newDataFrame = do
  let updatedDatabase = updateDatabase name newDataFrame 
  fileContent <- Pure $ saveDatabaseToJSON updatedDatabase
  saveFile name fileContent

-- updateAndSave :: TableName -> DataFrame -> IO DataFrame
-- updateAndSave tableName newDataFrame = do
--   let updatedDatabase = updateDatabase tableName newDataFrame 
--   saveDatabaseToJSON updatedDatabase
--   case lookup tableName updatedDatabase of
--     Just dataFrame -> return dataFrame
--     Nothing -> error "Table not found after update and save."

decideParser :: String -> Either ErrorMessage Statements
decideParser sql =
    case map toLower firstWord of
        "insert" -> Right ParseInsertStatement
        "select" -> Right ParseStatement
        "show" -> Right ParseStatement
        "delete" -> Right ParseDeleteStatement
        "update" -> Right ParseUpdateStatement
        _ -> Left "Unknown statement"
    where
        firstWord = takeWhile (/= ' ') sql

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
  case decideParser sql of
    Right statement -> 
      case statement of 
        ParseStatement -> 
          case parseStatement sql of
            Left errorMessage -> return $ Left errorMessage
            Right parsedStatement -> do
                case executeStatement parsedStatement of
                    Left errorMessage -> return $ Left errorMessage
                    Right result -> return $ Right result
        ParseInsertStatement -> 
          case parseInsertStatement sql of
              Right (Insert table columns values) -> case (insertStatement table columns values) of
                  Left errorMessage -> return $ Left errorMessage
                  Right result -> do
                    updateFile table result
                    return $ Right result
                    -- return $ Right (unsafePerformIO (updateAndSave table result))
              Left errorMessage -> return $ Left errorMessage
        ParseDeleteStatement ->
          case parseDeleteStatement sql of
              Right (Delete table limit) -> case (deleteStatement table limit) of
                Left errorMessage -> return $ Left errorMessage
                Right result -> do
                  updateFile table result
                  return $ Right result
                  -- return $ Right (unsafePerformIO (updateAndSave table result))
              Left errorMessage -> return $ Left errorMessage
        ParseUpdateStatement ->
          case parseUpdateStatement sql of
            Right (Update table setValues limit) -> case (updateStatement table setValues limit) of
              Left errorMessage -> return $ Left errorMessage
              Right result -> do
                updateFile table result
                return $ Right result
            Left errorMessage -> return $ Left errorMessage
    Left errorMessage -> return $ Left errorMessage
