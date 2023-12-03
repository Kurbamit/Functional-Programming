{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    -- readDatabaseFromJSON,
    saveDatabaseToJSON
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
import Data.Aeson.Encoding (value)
import Data.List (sortBy)
import Data.Functor.Identity (Identity)
import Data.List (find)
import GHC.IO (unsafePerformIO)
import Data.Char (toLower)
import Control.Monad.Trans.Error (Error)


type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type ColumnName = String

data ParsedStatement2
  = Insert TableName [ColumnName] [DataFrame.Value]
      deriving (Show, Eq)

data ParsedStatement3
  = Delete TableName Limit
    deriving (Show, Eq)

data Statements
  = ParseStatement
  | ParseStatement2
  | ParseStatement3
  deriving Show

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

whereConditionParser :: Parser Limit
whereConditionParser = do
  columnName <- alphanumericParser
  _ <- optional whiteSpaceParser
  _ <- stringParser "="
  _ <- optional whiteSpaceParser
  value <- alphanumericParser
  pure (Limit columnName (getValueType value))

deleteParser :: Parser ParsedStatement3
deleteParser = do
  _ <- stringParser "delete"
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

parseStatement3 :: String -> Either ErrorMessage ParsedStatement3
parseStatement3 input = case runParser parser input of
  Left errorMessage -> Left errorMessage
  Right (input, remainder) ->
    case input of
      Delete _ _ -> case runParser (optional whiteSpaceParser) remainder of
        Left errorMessage -> Left errorMessage
        Right _ -> Right input
  where
    parser :: Parser ParsedStatement3
    parser = deleteParser

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

insertParser :: Parser ParsedStatement2
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

parseStatement2 :: String -> Either ErrorMessage ParsedStatement2
parseStatement2 input = case runParser parser input of
  Left errorMessage -> Left errorMessage
  Right (input, remainder) ->
    case input of
      Insert _ _ _ -> case runParser (optional whiteSpaceParser) remainder of
        Left errorMessage -> Left errorMessage
        Right _ -> Right input
  where
    parser :: Parser ParsedStatement2
    parser = insertParser

updateDatabase :: TableName -> DataFrame -> Database
updateDatabase tableName newDataFrame = 
  (tableName, newDataFrame) : filter (\(name, _) -> name /= tableName) database

saveDatabaseToJSON :: Database -> IO ()
saveDatabaseToJSON updatedDatabase = do
  let filePath = "src/db/database.json"
      content = encode updatedDatabase
  BLC.writeFile filePath content

updateAndSave :: TableName -> DataFrame -> IO DataFrame
updateAndSave tableName newDataFrame = do
  let updatedDatabase = updateDatabase tableName newDataFrame 
  saveDatabaseToJSON updatedDatabase
  case lookup tableName updatedDatabase of
    Just dataFrame -> return dataFrame
    Nothing -> error "Table not found after update and save."

decideParser :: String -> Either ErrorMessage Statements
decideParser sql =
    case map toLower firstWord of
        "insert" -> Right ParseStatement2
        "select" -> Right ParseStatement
        "show" -> Right ParseStatement
        "delete" -> Right ParseStatement3
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
        ParseStatement2 -> 
          case parseStatement2 sql of
              Right (Insert table columns values) -> case (insertStatement table columns values) of
                  Left errorMessage -> return $ Left errorMessage
                  Right result -> return $ Right (unsafePerformIO (updateAndSave table result))
              Left errorMessage -> return $ Left errorMessage
        ParseStatement3 ->
          case parseStatement3 sql of
              Right (Delete table limit) -> case (deleteStatement table limit) of
                Left errorMessage -> return $ Left errorMessage
                Right result -> return $ Right result
              Left errorMessage -> return $ Left errorMessage
    Left errorMessage -> return $ Left errorMessage
