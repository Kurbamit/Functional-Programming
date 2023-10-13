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
import DataFrame (DataFrame)
import InMemoryTables (TableName, database)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SQLStatement SQLCommand
  deriving (Show, Eq)

data SQLCommand
  = ShowTables
  -- Define additional SQL commands like SUM, MIN, MAX here
  deriving (Show, Eq)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input = case map toUpper input of
  "SHOW TABLES" -> Right (SQLStatement ShowTables)
  -- Add parsing logic for other SQL commands here
  _ -> Left "Not implemented: parseStatement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage [TableName]
executeStatement (SQLStatement command) = case command of
  ShowTables -> Right $ map fst database
  -- Implement execution for other SQL commands here
  _ -> Left "Not implemented: executeStatement"

runSql :: String -> Either ErrorMessage [TableName]
runSql input = do
  parsed <- parseStatement input
  executeStatement parsed