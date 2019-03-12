{-# LANGUAGE OverloadedStrings #-}
module Storage (Record(..), get, set) where

import Control.Monad
import System.IO
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import qualified Data.ByteString.Base64 as Base64
import Data.Text
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.List as L
import Data.Maybe (maybe)
import Control.Monad.Trans.Class (lift)
import System.IO.Streams (InputStream)
import qualified System.IO.Streams as Streams
import Data.Either (Either(..), either)

type DBPath = FilePath
type Key = ByteString
type Value = ByteString
type EncodedRecord = ByteString


class Serializable a where
  serialize :: a -> EncodedRecord
  deserializeOnKeyMatch :: ByteString -> EncodedRecord -> Maybe a

rightToMaybe :: Either a b -> Maybe b
rightToMaybe = either (const Nothing) (Just)
  
data Record = Record {
      key :: ByteString
    , value :: ByteString
  } deriving (Show, Eq)

keyValueDelimiter :: ByteString
keyValueDelimiter = encodeUtf8 ","

instance Serializable Record where
  serialize (Record key value)= (Base64.encode key) <> keyValueDelimiter <> (Base64.encode value)
  deserializeOnKeyMatch key bs = let (eKey, eValue) = BS.break (== (L.head . BS.unpack . encodeUtf8 $ ",")) bs
                                     (eitherKey, eitherValue) = (Base64.decode eKey, Base64.decode . BS.tail $ eValue)
                                 in if key == eKey
                                      then rightToMaybe $ Record <$> eitherKey <*> eitherValue
                                      else Nothing 

dbFile :: DBPath
dbFile = "storage.log"

serializedRecords :: IO (InputStream EncodedRecord)
serializedRecords = do
  h <- openFile dbFile ReadMode
  Streams.makeInputStream (readLine h)
  where readLine :: Handle -> IO (Maybe EncodedRecord)
        readLine h = do
          heof <- hIsEOF h
          if heof
            then return Nothing
            else BS.hGetLine h >>= (return .Just)

get :: Serializable a => Key -> IO (Maybe a)
get key = do
  stream <- serializedRecords
  Streams.fold mostRecentKey Nothing stream
  where 
        mostRecentKey :: Serializable a => Maybe a -> EncodedRecord -> Maybe a
        mostRecentKey oldMatch record = let maybeMatch = deserializeOnKeyMatch encodedKey record
                                        in maybe oldMatch (const maybeMatch)  maybeMatch
        encodedKey = Base64.encode key

set ::  Serializable a => a -> IO ()
set r = BS.appendFile dbFile (serialize r <> recordsDelimiter)
  where recordsDelimiter :: ByteString
        recordsDelimiter = encodeUtf8 "\n"


testRecord :: Record
testRecord = Record "mehdi" "zonjy"






