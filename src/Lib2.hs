{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    runSql,
    ParsedStatement (..),
    SQLCommand (..)
  )
where

import Data.Char
import Data.Maybe (listToMaybe)
import Data.List (find, isPrefixOf)
import DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..))
import InMemoryTables (TableName, database)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SQLStatement SQLCommand
  deriving (Show, Eq)

data SQLCommand
  = ShowTables
  | ShowTableColumns TableName -- Add a new constructor for showing table columns
  -- Define additional SQL commands like SUM, MIN, MAX here
  deriving (Show, Eq)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | "show tables" `caseInsensitiveEquals` input = Right (SQLStatement ShowTables)
  | otherwise = case map toLower <$> words input of
    ["show", "tables"] -> Right (SQLStatement ShowTables)
    ["show", "table", tableName] -> Right (SQLStatement (ShowTableColumns tableName))
    _ -> Left "Not implemented: parseStatement"


-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (SQLStatement command) = case command of
  ShowTables -> Right $ DataFrame [Column "Tables" StringType] $ map (\(tableName, _) -> [StringValue tableName]) database
  ShowTableColumns tableName -> case findTable tableName database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "Columns" StringType] $ map (\(Column colName _) -> [StringValue colName]) columns
    Nothing -> Left "Table not found"
  -- Implement execution for other SQL commands here
  _ -> Left "Not implemented: executeStatement"


runSql :: String -> Either ErrorMessage [TableName]
runSql input = do
  parsed <- parseStatement input
  case executeStatement parsed of
    Right (DataFrame [Column _ StringType] rows) ->
      Right $ map (\(StringValue tableName : _) -> tableName) rows
    Left err -> Left err
    _ -> Left "Invalid result format"


-- Helper function to perform case-insensitive lookup
findTable :: TableName -> Database -> Maybe DataFrame
findTable targetTable database =
  listToMaybe [table | (tableName, table) <- database, targetTable `caseInsensitiveEquals` tableName]

-- Helper function to convert a string to lowercase
toLowerString :: String -> String
toLowerString = map toLower

-- Helper function to perform case-insensitive comparison
caseInsensitiveEquals :: String -> String -> Bool
caseInsensitiveEquals a b = toLowerString a == toLowerString b