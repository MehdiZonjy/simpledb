{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE Strict #-}

module Log(loadKeyDir, append, get, Record(..), KeyDir) where


import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import Data.Binary (put)
import Data.ByteString (ByteString (..))
import qualified Data.ByteString as BS
import Data.Word (Word32, Word16)
import Data.Binary.Put (Put(..), putWord16le, putWord32le, runPut, putByteString)
import Data.Binary.Get (Get ,getByteString, getWord16le, getWord32le, runGet, Decoder(..), runGetIncremental,pushChunk, skip, bytesRead, isEmpty )
import qualified Data.ByteString.Lazy as LB
import Conduit
import Data.Conduit.Serialization.Binary (sourcePut, conduitDecode, conduitGet)
import Data.Time.Clock (getCurrentTime, utctDayTime)
import Data.Conduit.Combinators (sinkIOHandle, foldl)
import System.IO
import Data.Int (Int64(..))
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

data Record = Record {
    key :: !T.Text
  , value :: !T.Text
  } deriving (Eq, Show)


data LogRecord = LogRecord {
    lrCheck :: !Word32
  , lrTstamp :: !Word32
  , lrKeySize :: !Word16
  , lrValueSize :: !Word16
  , lrKey :: !ByteString
  , lrValue :: !ByteString
  }

type FileName = T.Text
type ValueSize = Word16
type KeySize = Word16
type ValuePosition = Int64
type TimeStamp = Word32
type Key = T.Text
type Value = T.Text

type KeyDir = M.Map Key ValueDescriptor


type ValueDescriptor = (FileName, ValueSize, ValuePosition, TimeStamp)
type LogValueDescriptor = (Key, ValueSize, ValuePosition, TimeStamp)


sumRecordCheck :: TimeStamp -> KeySize -> ValueSize -> ByteString -> ByteString -> Word32 
sumRecordCheck timestamp keySize valueSize key value = (sumBS key) + (sumBS value) + keySizeW32 + valueSizeW32 + timestamp
  where keySizeW32 :: Word32
        keySizeW32 = fromIntegral keySize
        valueSizeW32 :: Word32
        valueSizeW32 = fromIntegral valueSize


sumBS :: ByteString -> Word32
sumBS = BS.foldl' (\a w -> a + (fromIntegral w)) (0 :: Word32)

toLogRecord :: TimeStamp -> Record -> LogRecord
toLogRecord timestamp (Record k v) = LogRecord check timestamp keySize valueSize key value
  where
        keySize :: KeySize
        keySize = fromIntegral $ T.length k
        valueSize :: ValueSize
        valueSize = fromIntegral $ T.length v
        key = encodeUtf8 k
        value = encodeUtf8 v
        check :: Word32
        check = sumRecordCheck timestamp keySize valueSize key value

serializeLogRecord :: LogRecord -> Put
serializeLogRecord (LogRecord check timestamp keySize valueSize key value) = do
  putWord32le check
  putWord32le timestamp
  putWord16le keySize
  putWord16le valueSize
  putByteString  key
  putByteString value


deserializeLogValueDescriptor :: Get LogValueDescriptor
deserializeLogValueDescriptor  = do
  check <- getWord32le
  timestamp <- getWord32le
  keySize <-  getWord16le
  valueSize <- getWord16le
  keyBS <- getByteString $ fromIntegral keySize
  valueBS <- getByteString $ fromIntegral valueSize
  let key = decodeUtf8 keyBS
      keyCheck = sumBS keyBS
      valuePos :: Int64
      valuePos = 4 + 4 + 2 + 2 + (fromIntegral keySize)
  if (check == sumRecordCheck timestamp keySize valueSize keyBS valueBS)
    then return (key, valueSize, valuePos, timestamp)
    else fail "record is corrupted"


shiftValuePos :: ValuePosition -> LogValueDescriptor -> LogValueDescriptor
shiftValuePos offset (key, valueSize, valuePosition, timeStamp) =  (key, valueSize, valuePosition + offset , timeStamp)

getLogValueDescriptor :: MonadThrow m => ConduitT ByteString LogValueDescriptor m ()
getLogValueDescriptor  = start 0
  where
    start :: MonadThrow m => ValuePosition -> ConduitT ByteString LogValueDescriptor m ()
    start offset = do !mx <- await
                      case mx of
                        !Nothing -> return ()
                        Just !x -> go offset (runGetIncremental deserializeLogValueDescriptor `pushChunk` x)
    go :: MonadThrow m => ValuePosition -> Decoder LogValueDescriptor -> ConduitT ByteString LogValueDescriptor m ()
    go offset !(!Done !bs consumed !logValueDescriptor) = do  yield (shiftValuePos offset logValueDescriptor)
                                                              if BS.null bs
                                                                then start (offset + consumed)
                                                                else go (offset + consumed) (runGetIncremental deserializeLogValueDescriptor `pushChunk` bs)
    go _ !(!Fail !u !o !e)  = return ()
    go offset !(!Partial !n)   = await >>= ((go offset) . n)


truncateFile :: Integer -> FileName -> IO ()
truncateFile offset file = do
  h <- openFile (T.unpack file) ReadWriteMode
  hSetFileSize h $   offset
  hClose h


buildKeyDir :: Monad m => FileName -> ConduitT LogValueDescriptor o m (KeyDir, Maybe LogValueDescriptor)
buildKeyDir fileName = Data.Conduit.Combinators.foldl store (M.empty, Nothing)
  where
    store :: (KeyDir, Maybe LogValueDescriptor) -> LogValueDescriptor -> (KeyDir, Maybe LogValueDescriptor)
    store (map, _) val@(key, valueSize, valuePosition, timeStamp)= (M.insert key (fileName, valueSize, valuePosition, timeStamp) map, Just val)

loadKeyDir :: IO KeyDir
loadKeyDir = do
  let fileName = T.pack activeLog
  (map, maybeLastRecord) <- runConduitRes $! (sourceFile  . T.unpack $ fileName) .| getLogValueDescriptor .| (buildKeyDir fileName)
  case maybeLastRecord of
    Nothing -> truncateFile 0  fileName
    Just (_, valSize, valPos, _) -> truncateFile (fromIntegral valSize + (fromIntegral valPos)) fileName
  return map




append :: Record -> IO ()
append rec = do
  h <- openFile activeLog AppendMode
  currTime <- getCurrentTime
  let timestamp = floor $ utctDayTime currTime :: Word32
      log = toLogRecord timestamp rec
      bs = runPut $ serializeLogRecord log
  BL.hPut h bs
  hClose h

get :: KeyDir -> Key -> IO (Maybe Value)
get map key = do
  let maybeValDesc = M.lookup key map
  case maybeValDesc of
    Nothing -> return Nothing
    Just (fieName, valSize, valPos, _) -> do h <- openFile activeLog ReadMode
                                             hSeek h AbsoluteSeek (fromIntegral valPos)
                                             valBS <- BS.hGet h (fromIntegral valSize)
                                             hClose h
                                             return . Just . decodeUtf8 $ valBS

activeLog = "log.bin"
