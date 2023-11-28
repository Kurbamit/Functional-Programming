{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib2
  (
    parseStatement,
    executeStatement,
    runSql,
    ColumnWithAggregate (..),
    Aggregate (..),
    Limit (..),
    columnNamesToRows,
    ParsedStatement (..),
  )
where

import Data.Char
import Data.Maybe (listToMaybe, isJust, fromMaybe)
import Data.List (find, isPrefixOf)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import InMemoryTables (TableName, database, tableLongStrings)
import Lib1 (parseSelectAllStatement)
import Data.List (elemIndex)
import Data.List (isInfixOf)
import Data.List (sum)
import Data.List (find)
import Data.List (nub)
import GHC.Generics (D)
import Control.Monad.Trans.Error (Error)
import Data.List (foldl')
import Text.ParserCombinators.ReadP (string, sepBy, char, option)
import Data.Char (isSpace)
import GHC.OldList (dropWhileEnd)
import Control.Alternative.Free


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data ColumnWithAggregate = ColumnWithAggregate String (Maybe Aggregate)
  deriving (Show, Eq)

data Aggregate
  = Max
  | Sum
  deriving (Show, Eq)

data Limit = Limit String Value
  deriving (Show, Eq)

data ParsedStatement
  = ShowTables {}
  | ShowTable TableName
  | Select [ColumnWithAggregate] [TableName] [Limit]
    deriving (Show, Eq)

class Applicative f => Alternative f where
  empty :: f a
  (<|>) :: f a -> f a -> f a

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (a, String)
  }

instance Functor Parser where
  fmap f (Parser x) = Parser $ \s -> do
    (result, newState) <- x s
    return (f result, newState)

instance Applicative Parser where
  pure x = Parser $ \input -> Right (x, input)
  (Parser pf) <*> (Parser pa) = Parser $ \input ->
    case pf input of
      Left err -> Left err
      Right (f, rest) -> case pa rest of
        Left err -> Left err
        Right (a, rest') -> Right (f a, rest')

instance Monad Parser where
  (Parser x) >>= f = Parser $ \s -> do
    (result, newState) <- x s
    runParser (f result) newState

instance Alternative Parser where
  empty = Parser $ \_ -> Left "No alternative"
  (Parser p1) <|> (Parser p2) = Parser $ \input ->
    case p1 input of
      Left _ -> p2 input
      result -> result

tryParser :: Parser a -> Parser a
tryParser (Parser p) = Parser $ \input ->
  case p input of
    Left errorMessage -> Left errorMessage
    Right (result, remaining) -> Right (result, remaining)

separate :: Parser a -> Parser b -> Parser [a]
separate p sep = (:) <$> p <*> many (sep *> p) <|> pure []

many :: Parser a -> Parser [a]
many p = go
  where
    go = (:) <$> p <*> go <|> pure []

optional :: Parser a -> Parser (Maybe a)
optional p = do
  Just <$> p
  <|> return Nothing

charParser :: Char -> Parser Char
charParser c = Parser parseC
  where parseC [] = Left "Empty or unfinished query"
        parseC (x : xs) | x == c = Right (c, xs)
                        | otherwise = Left ("Expected " ++ [c] ++ " in the query")

stringParser :: String -> Parser String
stringParser statement = Parser $ \input ->
  case take (length statement) input of
    [] -> Left "Empty or unfinished query"
    match
        | map toLower match == map toLower statement -> Right (match, drop (length match) input)
        | otherwise -> Left ("Expected '" ++ statement ++ "' in the query")

whiteSpaceParser :: Parser String
whiteSpaceParser = Parser $ \input ->
  case span isSpace input of
    ("", _ ) -> Left ("Expected whitespace before '" ++ input ++ "'")
    (whitespace, remainder) -> Right (whitespace, remainder)

showTablesParser :: Parser ParsedStatement
showTablesParser = tryParser $ do
  _ <- stringParser "show"
  _ <- whiteSpaceParser
  _ <- stringParser "tables"
  _ <- stringParser ";"
  pure ShowTables

showTableParser :: Parser ParsedStatement
showTableParser = tryParser $ do
  _ <- stringParser "show"
  _ <- whiteSpaceParser
  _ <- stringParser "table"
  _ <- whiteSpaceParser
  tableName <- alphanumericParser
  _ <- stringParser ";"
  pure (ShowTable tableName)

aggregateParser :: Parser (Maybe Aggregate)
aggregateParser = do
  maybeAggregate <- (stringParser "max(" *> pure (Just Max)) <|> (stringParser "sum(" *> pure (Just Sum)) <|> pure Nothing
  pure maybeAggregate

alphanumericParser :: Parser String
alphanumericParser = Parser $ \input ->
  case runParser (stringParser "now()") input of 
    Right rest -> Right rest
    Left _ -> case takeWhile (\x -> isAlphaNum x || x == '*') input of
      [] -> Left "Empty input"
      xs -> Right (xs, drop (length xs) input)


columnWithAggregateParser :: Parser ColumnWithAggregate
columnWithAggregateParser = do
  _ <- optional whiteSpaceParser
  maybeAggregate <- aggregateParser
  columnName <- alphanumericParser
  _ <- optional (stringParser ")")
  pure (ColumnWithAggregate columnName maybeAggregate)

columnSectionParser :: Parser [ColumnWithAggregate]
columnSectionParser = separate columnWithAggregateParser (stringParser ",")

selectTableParser :: Parser TableName
selectTableParser = do
  _ <- optional whiteSpaceParser
  table <- alphanumericParser
  pure table

tableSectionParser :: Parser [TableName]
tableSectionParser = separate selectTableParser (stringParser ",")

getValueType :: String -> Value
getValueType value
  | all isDigit value = IntegerValue (read value)
  | map toLower value == "true" = BoolValue True
  | map toLower value == "false" = BoolValue False
  | null value || all isSpace value = NullValue
  | otherwise = StringValue value

whereConditionParser :: Parser Limit
whereConditionParser = do
  _ <- optional whiteSpaceParser
  columnName <- alphanumericParser
  _ <- optional whiteSpaceParser
  _ <- stringParser "="
  _ <- optional whiteSpaceParser
  value <- alphanumericParser
  _ <- optional whiteSpaceParser
  pure (Limit columnName (getValueType value))

whereSectionParser :: Parser [Limit]
whereSectionParser = separate whereConditionParser (stringParser "or")

selectParser :: Parser ParsedStatement
selectParser = tryParser $ do
  _ <- stringParser "select"
  _ <- whiteSpaceParser
  columns <- columnSectionParser
  _ <- whiteSpaceParser
  _ <- stringParser "from"
  _ <- whiteSpaceParser
  tables <- tableSectionParser
  conditions <- optional $ do
    _ <- whiteSpaceParser
    _ <- stringParser "where"
    _ <- whiteSpaceParser
    conditions <- whereSectionParser
    pure conditions
  _ <- stringParser ";"
  _ <- optional whiteSpaceParser
  pure (Select columns tables (fromMaybe [] conditions))

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = case runParser parser input of
  Left errorMessage -> Left errorMessage
  Right (input, remainder) ->
    case input of
      ShowTables -> validateRemainder remainder >> Right input
      ShowTable _ -> validateRemainder remainder >> Right input
      Select _ _ _ -> validateRemainder remainder >> Right input
  where
    parser :: Parser ParsedStatement
    parser = showTablesParser <|> showTableParser <|> selectParser

    validateRemainder :: String -> Either ErrorMessage ()
    validateRemainder remainder =
      case runParser (optional whiteSpaceParser) remainder of
        Left errorMessage -> Left errorMessage
        Right _ -> Right ()

dropWhiteSpaces :: String -> String
dropWhiteSpaces = filter (not . isSpace)

columnNamesToRows :: [Column] -> [Row]
columnNamesToRows = map (\(Column name _) -> [StringValue name])

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

getColumnNames :: [ColumnWithAggregate] -> [String]
getColumnNames columns  = [name | ColumnWithAggregate name _ <- columns]

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


validateDatabaseColumns :: [String] -> [String] -> Bool
validateDatabaseColumns tables columns =
  all (\column ->
    any (\table ->
      case lookup table InMemoryTables.database of
        Just (DataFrame tableColumns _) ->
          any (\(Column name _) -> (name == column || column == "*")) tableColumns
        Nothing -> False
    ) tables
  ) columns

validateDatabaseTables :: [String] -> Bool
validateDatabaseTables tables =
  all (\table -> case (findTable table database) of
    Just (DataFrame _ _) -> True
    Nothing -> False
  ) tables

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement parsedStatement = case parsedStatement of
  ShowTables -> Right $ DataFrame [Column "Tables" StringType] $ map (\(tableName, _) -> [StringValue tableName]) database
  ShowTable tableName -> case findTable tableName database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "Columns" StringType] $ map (\(Column colName _) -> [StringValue colName]) columns
    Nothing -> Left "TABLE not found"
  Select columns tables conditions -> 
    case validateDatabaseTables tables of 
      True -> case validateDatabaseColumns tables (getColumnNames columns) of
        True -> case (validateDatabaseColumns tables (extractNamesFromLimits conditions)) of
          True -> combineDataFrames (selectMultipleTables tables columns conditions)
          False -> Left $ "COLUMNS specified in WHERE clause do not exists in database"
        False -> Left $ "Such COLUMNS do not exist in database"
      False -> Left $ "Such TABLES do not exist in database"
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