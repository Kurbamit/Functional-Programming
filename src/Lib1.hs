{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (DataFrame), ColumnType (IntegerType, StringType, BoolType), Value (IntegerValue, StringValue, BoolValue, NullValue), Column (Column))
import InMemoryTables (TableName)
import Data.Char (toLower)
import Data.List (isPrefixOf, transpose)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing  -- If the database is empty, return Nothing
findTableByName ((tableName, frame) : rest) name
  | map toLower tableName == map toLower name = Just frame  -- Case-insensitive comparison
  | otherwise = findTableByName rest name  -- Continue searching in the rest of the database

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
-- Checking the format of SELECT statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement statement 
  | null statement = Left "Input is empty"
  | isValid statement = Right (getTableName statement)
  | otherwise = Left "Invalid format"

-- Check if the statement starts with a valid prefix
isValid :: String -> Bool
isValid statement = "select * from " `isPrefixOf` map toLower statement

-- Extract the table name
getTableName :: String -> String
getTableName statement =
  let lastCharacter = last statement
  in
    if lastCharacter == ';'
      then drop 14 (init statement)
      else drop 14 statement

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..

validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame df@(DataFrame _ _) =
  case (checkEveryColumn  df, checkRowsLength df) of
    (True, True) -> Right ()
    (_, False) -> Left "Row lengths do not match the number of columns"
    (False, _) -> Left "Columns have mixed data types"

validateColumnValues :: ColumnType -> [Value] -> Bool
validateColumnValues columnType = all (\value ->
    case (columnType, value) of
      (IntegerType, IntegerValue _) -> True
      (StringType, StringValue _)   -> True
      (BoolType, BoolValue _)       -> True
      (_, NullValue)                -> True
      _                             -> False
    )

checkEveryColumn :: DataFrame -> Bool
checkEveryColumn (DataFrame columns values) = 
  all (\(Column _ columnType, columnValues) -> validateColumnValues columnType columnValues)
  (zip columns (transpose values))

checkRowsLength :: DataFrame -> Bool
checkRowsLength (DataFrame columns rows) = allRowsHaveCorrectLength
  where
    allRowsHaveCorrectLength = all (\row -> length row == length columns) rows

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)

{-
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ (DataFrame columns rows) =
  let
    -- Calculate the maximum width for each column
    columnWidths = map (maximum . map valueWidth) (transpose rows)

    -- Create a separator row
    separator = "+-" ++ concatMap (`replicate` '-') columnWidths ++ "-+\n"

    -- Render the header row
    headerRow = renderRow columnWidths (map columnNameAndType columns)

    -- Render each data row
    dataRows = map (renderRow columnWidths . map valueToString) rows
  in
    unlines (separator : headerRow : separator : dataRows ++ [separator])

-- Calculate the width of a value for column alignment
valueWidth :: Value -> Int
valueWidth NullValue = 4
valueWidth (IntegerValue x) = length (show x)
valueWidth (StringValue s) = length s
valueWidth (BoolValue _) = 5

-- Render a row of values with proper column alignment
renderRow :: [Int] -> [String] -> String
renderRow widths values =
  "| " ++ concatMap (\(value, width) -> padValue value width ++ " | ") (zip values widths)

-- Pad a value to the specified width
padValue :: String -> Int -> String
padValue value width =
  let padding = width - length value
  in value ++ replicate padding ' '

-- Get column name and type as a string
columnNameAndType :: Column -> String
columnNameAndType (Column name colType) = name ++ " (" ++ show colType ++ ")"

-- Convert a Value to a String
valueToString :: Value -> String
valueToString NullValue = "NULL"
valueToString (IntegerValue x) = show x
valueToString (StringValue s) = s
valueToString (BoolValue True) = "TRUE"
valueToString (BoolValue False) = "FALSE"
-}