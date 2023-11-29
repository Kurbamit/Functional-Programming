{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    -- readDatabaseFromJSON,
    -- saveDatabaseToJSON,
  )
where

import Control.Monad.Free (Free (..), liftF)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Aeson as Aeson
import GHC.Generics (Generic)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..))
import Data.Time ( UTCTime )
import Lib2
import Data.Functor.Classes (readData)


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


type Database = [(TableName, DataFrame)]

readDatabaseFromJSON :: FilePath -> IO (Either String Database)
readDatabaseFromJSON filePath = do
  content <- BLC.readFile filePath
  return $ eitherDecode content

saveDatabaseToJSON :: FilePath -> Database -> IO ()
saveDatabaseToJSON filePath database = do
  let content = encode database
  BLC.writeFile filePath content