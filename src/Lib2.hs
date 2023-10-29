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
import Data.List (elemIndex)
import Data.List (isInfixOf)
import Data.List (sum)
import Data.List (find)
import GHC.Generics (D)


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SQLStatement SQLCommand
  deriving (Show, Eq)

data SQLCommand
  = ShowTables
  | ShowTableColumns TableName -- Add a new constructor for showing table columns
  | Select TableName [ColumnWithAggregate] [Limit]
  -- Define additional SQL commands like SUM, MIN, MAX here
  deriving (Show, Eq)

data ColumnWithAggregate = ColumnWithAggregate String (Maybe Aggregate)
  deriving (Show, Eq)

data Aggregate
  = Max
  | Sum
  deriving (Show, Eq)

data Limit = Limit String Value
  deriving (Show, Eq)

-- -- Helper function for testing purpose to convert a list of Column into rows of Value
columnNamesToRows :: [Column] -> [Row]
columnNamesToRows = map (\(Column name _) -> [StringValue name])

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  if last input == ';'
    then 
      let lowerCaseInput = map toLower (take (returnStartIndex (findSubstringPosition input ";")) input)
      in case words lowerCaseInput of
          ["show", "tables"] -> Right (SQLStatement ShowTables)
          ["show", "table", tableName] -> Right (SQLStatement (ShowTableColumns (extractSubstring input tableName)))
          ("select" : rest) ->
            if substringExists "from" lowerCaseInput 
            then 
                let tableName = extractTableName 1 rest
                in
                case countCommasInList rest of
                    n | n == (getPosition tableName - 2) ->
                        let columns = formColumnWithAggregateList (take (returnStartIndex (findSubstringPosition lowerCaseInput "from")) (removeAllCommas input)) (removeCommas (take (length rest - (length rest - getPosition tableName) - 1) rest))
                            limits = extractLimits (drop (returnStartIndex (findSubstringPosition lowerCaseInput "where")) input) (take (length rest - getPosition tableName - 1) (drop (getPosition tableName + 1) rest))
                        in
                        Right (SQLStatement (Select (extractSubstring input (getName tableName)) columns limits))
                    _ -> Left "Missing (,) after SELECT"
            else
                Left "Missing FROM clause"
          _ -> Left "Not implemented: parseStatement"
    else Left "Missing (;) after statement"

substringExists :: String -> String -> Bool
substringExists substring string = isInfixOf substring string

countCommasInList :: [String] -> Int
countCommasInList strings = sum $ map (length . filter (== ',')) strings

removeCommas :: [String] -> [String]
removeCommas = map removeComma
  where
    removeComma :: String -> String
    removeComma str =
        let reversedStr = reverse str
            reversedStrWithoutCommas = dropWhile (== ',') reversedStr
        in reverse reversedStrWithoutCommas

removeAllCommas :: String -> String
removeAllCommas = filter (/= ',')

extractTableName :: Int -> [String] -> (String, Int)
extractTableName index ("from" : tableName : _) = (tableName, index)
extractTableName index (_ : columns) = extractTableName (index + 1) columns

getName :: (String, Int) -> String
getName (name, _) = name

getPosition :: (String, Int) -> Int
getPosition (_, position) = position

extractLimits :: String -> [String] -> [Limit]
extractLimits _ [] = []
extractLimits input ("where" : rest) = extractLimits input rest
extractLimits input ("or" : rest) = extractLimits input rest
extractLimits input (columnName : "=" : value : rest) =
  let originalColumnName = extractSubstring input columnName
  in Limit originalColumnName (getValueType (extractSubstring input value)) : extractLimits input rest

getValueType :: String -> Value
getValueType value
  | all isDigit value = IntegerValue (read value)
  | map toLower value == "true" = BoolValue True
  | map toLower value == "false" = BoolValue False
  | null value || all isSpace value = NullValue
  | otherwise = StringValue value

formColumnWithAggregateList :: String -> [String] -> [ColumnWithAggregate]
formColumnWithAggregateList input inputWords = map (\word -> extractNameFromAggregate input word) inputWords

extractNameFromAggregate :: String -> String -> ColumnWithAggregate
extractNameFromAggregate input word
    | "max(" `isPrefixOf` word = ColumnWithAggregate (extractSubstring input (drop 4 (take (length word - 1) word))) (Just Max)
    | "sum(" `isPrefixOf` word = ColumnWithAggregate (extractSubstring input (drop 4 (take (length word - 1) word))) (Just Sum)
    | otherwise = ColumnWithAggregate (extractSubstring input word) Nothing

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

returnStartIndex :: (Int, Int) -> Int
returnStartIndex (start, _) = start

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

applyLimits :: DataFrame -> [Limit] -> Either ErrorMessage DataFrame
applyLimits (DataFrame tableColumns tableRows) [] = Right (DataFrame tableColumns tableRows)
applyLimits (DataFrame tableColumns tableRows) limits =
  if allColumnNamesExistInDataFrame (extractNamesFromLimits limits) tableColumns
    then Right (DataFrame tableColumns filteredRows)
    else Left ("Column(s) not found (WHERE clause)")
  where
      filteredRows = concatMap (\limit -> applyLimit limit tableRows) limits

applyLimit :: Limit -> [Row] -> [Row]
applyLimit (Limit _ value) rows = filter (elem value) rows

allColumnNamesExistInDataFrame :: [String] -> [Column] -> Bool
allColumnNamesExistInDataFrame columnNames columns = all (\columnName -> any (\(Column name _) -> name == columnName) columns) columnNames

extractNamesFromLimits :: [Limit] -> [String]
extractNamesFromLimits limits = [name | Limit name _ <- limits]

selectFromTable :: TableName -> [ColumnWithAggregate] -> [Limit] -> Database -> Either ErrorMessage DataFrame
selectFromTable tableName columns limits database =
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
             in case applyLimits (DataFrame requestedColumns selectedRows) limits of
              Right dataFrame -> Right (applyAggregates dataFrame columns)
              Left errorMessage -> Left errorMessage
    Nothing -> Left "Table not found"

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
  Select tableName columns limits -> selectFromTable tableName columns limits database
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