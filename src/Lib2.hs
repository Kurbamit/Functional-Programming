{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib2
  ( parseStatement,
    executeStatement,
    runSql,
    ColumnWithAggregate (..),
    Aggregate (..),
    Limit (..),
    columnNamesToRows,
    ParsedStatement (..),
    SQLCommand (..)
  )
where

import Data.Char
import Data.Maybe (listToMaybe, isJust)
import Data.List (find, isPrefixOf)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import InMemoryTables (TableName, database)
import Lib1 (parseSelectAllStatement)
import Data.List (elemIndex)
import Data.List (isInfixOf)
import Data.List (sum)
import Data.List (find)
import Data.List (nub)
import GHC.Generics (D)
import Control.Monad.Trans.Error (Error)
import Data.List (foldl')


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SQLStatement SQLCommand
  deriving (Show, Eq)

data SQLCommand
  = ShowTables
  | ShowTableColumns TableName -- Add a new constructor for showing table columns
  | Select [TableName] [ColumnWithAggregate] [Limit]
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
parseStatement input = do
    if last input == ';'
        then do
            let lowerCaseInput = map toLower (take (returnStartIndex (findSubstringPosition input ";")) input)
            case words lowerCaseInput of
                ["show", "tables"] -> Right (SQLStatement ShowTables)
                ["show", "table", tableName] -> Right (SQLStatement (ShowTableColumns (extractSubstring input tableName)))
                ("select" : rest) ->
                    if substringExists "from" lowerCaseInput 
                    then do
                      let tableNames = extractTableNames 1 rest
                        in if null (getNames tableNames)
                          then 
                            Left "Table not found"
                          else
                            case countCommasInList rest of
                                n | n == (getPosition tableNames - 2) ->
                                    let result = formColumnWithAggregateList (take (returnStartIndex (findSubstringPosition lowerCaseInput "from")) (removeAllCommas input)) (removeCommas (take (length rest - (length rest - getPosition tableNames) - 1) rest))
                                    in case result of
                                        Right columns ->
                                            let limits = extractLimits (drop (returnStartIndex (findSubstringPosition lowerCaseInput "where")) input) (take (length rest - getPosition tableNames - 1) (drop (findWherePosition rest) rest))
                                            in case limits of
                                              Right limits ->
                                                 Right (SQLStatement (Select (map (\tableName -> extractSubstring input tableName) (take (findWherePosition (getNames tableNames)) (getNames tableNames))) columns limits))
                                              Left errorMessage -> Left errorMessage
                                        Left errorMessage -> Left errorMessage
                                n | n > (getPosition tableNames - 2) -> Left "No column names were found (SELECT clause)"
                                _ -> Left "Missing (,) after SELECT"
                    else
                        Left "Missing FROM clause"
                _ -> Left "Not implemented: parseStatement"
        else Left "Missing (;) after statement"

findWherePosition :: [String] -> Int
findWherePosition rest =
  case span (/= "where") rest of
    (_, [])   -> length rest
    (beforeWhere, _) -> length beforeWhere 

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

extractTableNames :: Int -> [String] -> ([String], Int)
extractTableNames index [] = ([], index)
extractTableNames index ("from" : tableName : rest) = (tableName : rest, index)
extractTableNames index (_ : columns) = extractTableNames (index + 1) columns

getNames :: ([String], Int) -> [String]
getNames (names, _) = names

getPosition :: ([String], Int) -> Int
getPosition (_, position) = position

extractLimits :: String -> [String] -> Either ErrorMessage [Limit]
extractLimits _ [] = Right []  -- Empty list is a valid case, so return Right
extractLimits input ("where" : rest) = extractLimits input rest
extractLimits input ("or" : rest) = extractLimits input rest
extractLimits input (columnName : equalitySign : value : rest)
  | validateEqualitySign equalitySign = do
    let originalColumnName = extractSubstring input columnName
    let valueType = getValueType (extractSubstring input value)
    restLimits <- extractLimits input rest
    return (Limit originalColumnName valueType : restLimits)
  | otherwise = Left "'=' was not found"


validateEqualitySign :: String -> Bool
validateEqualitySign "=" = True;
validateEqualitySign _ = False;

getValueType :: String -> Value
getValueType value
  | all isDigit value = IntegerValue (read value)
  | map toLower value == "true" = BoolValue True
  | map toLower value == "false" = BoolValue False
  | null value || all isSpace value = NullValue
  | otherwise = StringValue value

formColumnWithAggregateList :: String -> [String] -> Either ErrorMessage [ColumnWithAggregate]
formColumnWithAggregateList _ [] = Left "No column names were found (SELECT clause)"
formColumnWithAggregateList input inputWords = do
    let columnAggregates = map (extractNameFromAggregate input) inputWords
    checkMultipleAggregates columnAggregates

checkMultipleAggregates :: [ColumnWithAggregate] -> Either ErrorMessage [ColumnWithAggregate]
checkMultipleAggregates columns =
    let aggregates = filter (\(ColumnWithAggregate _ maybeAgg) -> isJust maybeAgg) columns
    in if length aggregates > 1
        then Left "Multiple aggregate functions are not allowed."
        else Right columns

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
    then Right (DataFrame tableColumns (filteredRows))
    else Left ("Column(s) not found (WHERE clause)")
  where
      filteredRows = concatMap (\limit -> applyLimit limit tableRows) limits

applyLimit :: Limit -> [Row] -> [Row]
applyLimit (Limit _ value) rows = filter (elem value) rows

allColumnNamesExistInDataFrame :: [String] -> [Column] -> Bool
allColumnNamesExistInDataFrame columnNames columns = all (\columnName -> any (\(Column name _) -> name == columnName) columns) columnNames

extractNamesFromLimits :: [Limit] -> [String]
extractNamesFromLimits limits = [name | Limit name _ <- limits]

extractNameFromLimit :: Limit -> String
extractNameFromLimit (Limit name _) = name

selectFromTable :: TableName -> [ColumnWithAggregate] -> [Limit] -> Database -> Either ErrorMessage DataFrame
selectFromTable tableName columns limits database =
  case findTable tableName database of
    Just (DataFrame tableColumns tableRows) ->
      if "*" `elem` map getColumnName columns
      then
        case applyLimits (DataFrame tableColumns tableRows) limits of 
          Right dataframe -> Right dataframe
          Left errorMessage -> Left errorMessage
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

selectMultipleTables :: [TableName] -> [ColumnWithAggregate] -> [Limit] -> [Either ErrorMessage DataFrame]
selectMultipleTables tableNames columns limits =
  map (\tableName -> selectFromTable tableName (filterTableColumns tableName columns) (filterTableLimits tableName limits) database) tableNames

filterTableColumns :: String -> [ColumnWithAggregate] -> [ColumnWithAggregate]
filterTableColumns tableName columns =
  case findTable tableName database of
    Just (DataFrame tableColumns _) ->
      case columns of
        (column : rest) ->
          if getColumnName column == "*"
            then columns
            else filter (\col -> getColumnName col `elem` map extractColumnName tableColumns) columns

extractColumnName :: Column -> String
extractColumnName (Column name _) = name

filterTableLimits :: String -> [Limit] -> [Limit]
filterTableLimits tableName limits =
  case findTable tableName database of
    Just (DataFrame tableColumns _) ->
      filter (\limit -> extractNameFromLimit limit `elem` map extractColumnName tableColumns) limits

combineDataFrames :: [Either ErrorMessage DataFrame] -> Either ErrorMessage DataFrame
combineDataFrames dataFrames =
  case sequence dataFrames of
  Left errorMessage -> Left errorMessage
  Right frames ->
      case foldr (\dataframe acc -> acc >>= \acc' -> join dataframe acc') (Right emptyDataFrame) frames of
        Left errorMessage -> Left errorMessage
        Right dataframe -> Right dataframe

emptyDataFrame :: DataFrame
emptyDataFrame = DataFrame [] [[]]

join :: DataFrame -> DataFrame -> Either ErrorMessage DataFrame
join (DataFrame cols1 rows1) (DataFrame cols2 rows2) = do
  combinedCols <- combineColumns cols1 cols2
  combinedRows <- combineRows rows1 rows2
  return (DataFrame combinedCols combinedRows)

combineColumns :: [Column] -> [Column] -> Either ErrorMessage [Column]
combineColumns cols1 cols2
  | hasCommonColumns cols1 cols2 = Left "Columns in both DataFrames have the same names"
  | otherwise = Right $ cols1 ++ cols2

hasCommonColumns :: [Column] -> [Column] -> Bool
hasCommonColumns cols1 cols2 = any (\(Column name1 _) -> any (\(Column name2 _) -> name1 == name2) cols2) cols1

combineRows :: [Row] -> [Row] -> Either ErrorMessage [Row]
combineRows rows1 rows2 = Right [v1 ++ v2 | v1 <- rows1, v2 <- rows2]

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (SQLStatement command) = case command of
  ShowTables -> Right $ DataFrame [Column "Tables" StringType] $ map (\(tableName, _) -> [StringValue tableName]) database
  ShowTableColumns tableName -> case findTable tableName database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "Columns" StringType] $ map (\(Column colName _) -> [StringValue colName]) columns
    Nothing -> Left "Table not found"
  Select tableNames columns limits -> combineDataFrames (selectMultipleTables tableNames columns limits)
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