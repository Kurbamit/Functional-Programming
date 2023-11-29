{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import qualified Data.ByteString.Lazy as BL
import Data.Aeson as Aeson
import GHC.Generics (Generic)
import DataFrame (DataFrame)
import Data.Time ( UTCTime )
import Lib2

data TableEmployees = TableEmployees
  {
    tableName :: String
  } deriving (Generic, FromJSON)

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    case parseStatement sql of
        Left errorMessage -> return $ Left errorMessage
        Right parsedStatement -> do
            case executeStatement parsedStatement of
                Left errorMessage -> return $ Left errorMessage
                Right result -> return $ Right result

getTableNameFromFile :: FilePath -> IO ()
getTableNameFromFile fileName = do
    let filePath = "src/db/" ++ fileName ++ ".json"
    content <- BL.readFile filePath
    let parsedContent = Aeson.decode content :: Maybe TableEmployees
    case parsedContent of
      Nothing -> error $ "Could not parse file " ++ filePathK
      Just table -> putStr $ "Table name for " ++ filePath ++ " is " ++ tableName table