{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame)
import InMemoryTables (TableName)
import Data.Char (toLower)
import Data.List (isPrefixOf)

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
isValid statement = "select * from " `isPrefixOf` (map toLower statement)

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
validateDataFrame _ = error "validateDataFrame ot implemented"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
