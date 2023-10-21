{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

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
import Lib1 (parseSelectAllStatement)
import GHC.Conc (par)
import System.IO (putStrLn)
import Debug.Trace ( trace )
import Data.List (elemIndex)
import Data.List (isInfixOf)
import Text.ParserCombinators.ReadP (string)


type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SQLStatement SQLCommand
  deriving (Show, Eq)

data SQLCommand
  = ShowTables
  | ShowTableColumns TableName -- Add a new constructor for showing table columns
  | Select TableName [String] 
  -- Define additional SQL commands like SUM, MIN, MAX here
  deriving (Show, Eq)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
    let lowerCaseInput = map toLower input
    in case words lowerCaseInput of
        ["show", "tables"] -> Right (SQLStatement ShowTables)
        ["show", "table", tableName] -> Right (SQLStatement (ShowTableColumns (extractSubstring input tableName)))
        ["select", columns, "from", tableName] -> Right (SQLStatement (Select (extractSubstring input tableName) (words columns)))
        _ -> Left "Not implemented: parseStatement"

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

selectFromTable :: TableName -> [String] -> Database -> Either ErrorMessage DataFrame
selectFromTable tableName columns database = do
  case findTable tableName database of
    Just (DataFrame tableColumns tableRows) -> do
      let selectedColumns = filterColumns tableColumns columns
      let selectedRows = map (\row -> filterRow row (getColumnsWithIndexes selectedColumns tableColumns)) tableRows
      return (DataFrame selectedColumns selectedRows)
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