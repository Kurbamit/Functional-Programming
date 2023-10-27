{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib2
  ( parseStatement,
    executeStatement,
    runSql,
    ColumnWithAggregate,
    columnNamesToRows,
    ParsedStatement (..),
    SQLCommand (..)
  )
where

import Data.Char
import Data.Maybe (listToMaybe)
import Data.List (find, isPrefixOf)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import InMemoryTables (TableName, database)
import Lib1 (parseSelectAllStatement)
import Debug.Trace ( trace, traceShow )
import Data.List (elemIndex)
import Data.List (isInfixOf)
import Data.List (sum)


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SQLStatement SQLCommand
  deriving (Show, Eq)

data SQLCommand
  = ShowTables
  | ShowTableColumns TableName -- Add a new constructor for showing table columns
  | Select TableName [ColumnWithAggregate]
  -- Define additional SQL commands like SUM, MIN, MAX here
  deriving (Show, Eq)

data ColumnWithAggregate = ColumnWithAggregate String (Maybe Aggregate)
  deriving (Show, Eq)

data Aggregate
  = Max
  | Sum
  deriving (Show, Eq)

-- -- Helper function for testing purpose to convert a list of Column into rows of Value
columnNamesToRows :: [Column] -> [Row]
columnNamesToRows = map (\(Column name _) -> [StringValue name])

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
    let lowerCaseInput = map toLower input
    in case words lowerCaseInput of
        ["show", "tables"] -> Right (SQLStatement ShowTables)
        ["show", "table", tableName] -> Right (SQLStatement (ShowTableColumns (extractSubstring input tableName)))
        ("select" : columnNames) -> do
            let tableName = extractTableName columnNames
            let columns = formColumnWithAggregateList (take (length columnNames - 2) columnNames)
            -- Commented part below extracts original column names from the input which helps to do CASE-SENSITIVE
            -- comparisons later. It is commented now because column names were represented as list of strings but that
            -- is not a case anymore. Column names will be CASE-INSENSITIVE untill we fix this
            Right (SQLStatement (Select (extractSubstring input tableName) {-map (extractSubstring input) -} columns))
        _ -> Left "Not implemented: parseStatement"

extractTableName :: [String] -> String
extractTableName ("from" : tableName : _) = tableName
extractTableName (column : columns) =
  let table = extractTableName columns in table

formColumnWithAggregateList :: [String] -> [ColumnWithAggregate]
formColumnWithAggregateList input = map extractNameFromAggregate input

extractNameFromAggregate :: String -> ColumnWithAggregate
extractNameFromAggregate input
    | "max(" `isPrefixOf` input = ColumnWithAggregate (drop 4 (take (length input - 1) input)) (Just Max)
    | "sum(" `isPrefixOf` input = ColumnWithAggregate (drop 4 (take (length input - 1) input)) (Just Sum)
    | otherwise = ColumnWithAggregate input Nothing

extractSubstring :: String -> String -> String
extractSubstring input stringToMach =
  if stringToMach `isInfixOf` input
    then stringToMach
    else extractFromOriginalString input (findSubstringPosition (map toLower input) stringToMach)
  where
    extractFromOriginalString :: String -> (Int, Int) -> String
    extractFromOriginalString originalString (start, end) =
      take (end - start + 1) (drop start originalString)

findSubstringPosition :: String -> String -> (Int, Int)
findSubstringPosition string substring = findPosition 0 string
  where
    findPosition :: Int -> String -> (Int, Int)
    findPosition index string@(x:xs)
      | substring `isPrefixOf` string = (index, index + length substring - 1)
      | otherwise = findPosition (index + 1) xs

findMax :: [Value] -> Value
findMax values@(value:_) =
  case value of
    IntegerValue _ -> maxInt values
    StringValue _ -> maxString values
    BoolValue _ -> maxBool values
    _ -> NullValue
    where
        maxInt :: [Value] -> Value
        maxInt values = do
            let valueList = [value | IntegerValue value <- values]
            if null valueList then
                NullValue
            else
                IntegerValue $ maximum valueList
        maxString :: [Value] -> Value
        maxString values = do
            let valueList = [value | StringValue value <- values]
            if null valueList then
                NullValue
            else
                StringValue $ maximum valueList
        maxBool :: [Value] -> Value
        maxBool values = do
            let valueList = [value | StringValue value <- values]
            if null valueList then
                NullValue
            else
                StringValue $ maximum valueList

findSum :: [Value] -> Value
findSum values@(value:_) =
  case value of
    IntegerValue _ -> sumInt values
    _ -> NullValue
    where
        sumInt :: [Value] -> Value
        sumInt values = do
            let valueList = [value | IntegerValue value <- values]
            if null valueList then
                NullValue
            else
                IntegerValue $ sum valueList

getColumnName :: ColumnWithAggregate -> String
getColumnName (ColumnWithAggregate name _) = name

getAggregate :: ColumnWithAggregate -> Maybe Aggregate
getAggregate (ColumnWithAggregate _ aggregate) = aggregate

findColumnByName :: DataFrame -> String -> Column
findColumnByName (DataFrame columns _) columnName =
    case filter (\(Column name _) -> name == columnName) columns of
        (column : _) -> column

findRows :: DataFrame -> String -> [Row]
findRows (DataFrame tableColumns tableRows) name = map (\row -> filterRow row (getColumnsWithIndexes [findColumnByName (DataFrame tableColumns tableRows)  name] tableColumns)) tableRows

applyAggregateToColumn :: DataFrame -> ColumnWithAggregate -> DataFrame
applyAggregateToColumn dataframe (ColumnWithAggregate _ Nothing) = dataframe
applyAggregateToColumn (DataFrame tableColumns tableRows) (ColumnWithAggregate name aggregate) =
  case getAggregate (ColumnWithAggregate name aggregate) of
    Just Max -> DataFrame tableColumns [valueToRow (findMax (rowToValue (findRows (DataFrame tableColumns tableRows) name)))]
    Just Sum -> DataFrame tableColumns [valueToRow (findSum (rowToValue (findRows (DataFrame tableColumns tableRows) name)))]

valueToRow :: Value -> Row
valueToRow val = [val]

rowToValue :: [Row] -> [Value]
rowToValue rows = concatMap (\row -> row) rows

applyAggregates :: DataFrame -> [ColumnWithAggregate] -> DataFrame
applyAggregates = foldr (\column dataFrame -> applyAggregateToColumn dataFrame column)

selectFromTable :: TableName -> [ColumnWithAggregate] -> Database -> Either ErrorMessage DataFrame
selectFromTable tableName columns database =
  case findTable tableName database of
    Just (DataFrame tableColumns tableRows) ->
      if "*" `elem` map getColumnName columns
      then
        let selectedRows = map (\row -> filterRow row (getColumnsWithIndexes tableColumns tableColumns)) tableRows
        in Right (DataFrame tableColumns selectedRows)
      else
        let columnNames = [name | Column name _ <- tableColumns]  -- Extract column names
            nonExistentColumns = filter (`notElem` columnNames)  (map getColumnName columns)
        in if not (null nonExistentColumns)
           then
             Left ("Column(s) not found: " ++ unwords nonExistentColumns)
           else
             let requestedColumns = filter (\(Column name _) -> name `elem` map getColumnName columns) tableColumns
                 selectedRows = map (\row -> filterRow row (getColumnsWithIndexes requestedColumns tableColumns)) tableRows
             in Right (applyAggregates (DataFrame requestedColumns selectedRows) columns)
    Nothing -> Left "Table not found"

filterColumns :: [Column] -> [String] -> [Column]
filterColumns allColumns selectedColumns
  | "*" `elem` selectedColumns = allColumns
  | otherwise = filter (\(Column name _) -> name `elem` selectedColumns) allColumns

getColumnsWithIndexes :: [Column] -> [Column] -> [(Int, Column)]
getColumnsWithIndexes selectedColumns allColumns =
  let getIndex column = case elemIndex column allColumns of
        Just index -> index
  in [(index, column) | column <- selectedColumns, let index = getIndex column]

filterRow :: Row -> [(Int, Column)] -> Row
filterRow row selectedColumns = [row !! index | (index, _) <- selectedColumns]

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (SQLStatement command) = case command of
  ShowTables -> Right $ DataFrame [Column "Tables" StringType] $ map (\(tableName, _) -> [StringValue tableName]) database
  ShowTableColumns tableName -> case findTable tableName database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "Columns" StringType] $ map (\(Column colName _) -> [StringValue colName]) columns
    Nothing -> Left "Table not found"
  Select tableName columns -> selectFromTable tableName columns database
  -- Implement execution for other SQL commands here
  _ -> Left "Not implemented: executeStatement"

runSql :: String -> Either ErrorMessage [TableName]
runSql input = do
  parsed <- parseStatement input
  case executeStatement parsed of
    Right (DataFrame [Column _ StringType] rows) ->
      Right $ map (\(StringValue tableName : _) -> tableName) rows
    Right _ -> Left "Invalid result format"
    Left err -> Left err

-- Helper function to perform case-sensitive lookup
findTable :: TableName -> Database -> Maybe DataFrame
findTable targetTable database =
  listToMaybe [table | (tableName, table) <- database, targetTable `caseSensitiveEquals` tableName]

-- Helper function to perform case-sensitive comparison
caseSensitiveEquals :: String -> String -> Bool
caseSensitiveEquals a b = a == b