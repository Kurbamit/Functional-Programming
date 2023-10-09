{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    showTables
  )
where

import DataFrame (DataFrame (..), Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName, tableEmployees, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls, database)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = ShowTables

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement _ = Left "Not implemented: parseStatement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement stmt =
  case stmt of
    -- Handle the SHOW TABLES statement
    ShowTables -> Right (DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database))
    -- Handle other statements here if needed
    _ -> Left "Statement not recognized"

showTables :: Database -> [TableName]
showTables db = map fst db