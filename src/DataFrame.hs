{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import GHC.Generics (Generic)
import Data.Aeson (FromJSON, ToJSON)

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data Column = Column String ColumnType
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq, Generic, FromJSON, ToJSON)
