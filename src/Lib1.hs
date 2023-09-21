{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

--import DataFrame (DataFrame, Value (BoolValue, IntegerValue, StringValue), ColumnType (..), Column (Column), Row)
import DataFrame (DataFrame (DataFrame), ColumnType (IntegerType, StringType, BoolType), Value (IntegerValue, StringValue, BoolValue, NullValue), Column (Column), Row)
import InMemoryTables (TableName)
import Data.Char (toLower)
import Data.List (isPrefixOf)
import Data.List (transpose)


type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

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
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
