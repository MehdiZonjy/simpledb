{-# LANGUAGE OverloadedStrings #-}

module Server where

import Network.Socket
import qualified Data.ByteString as BS
import Data.Binary.Get (Get ,getByteString, getWord8, getWord16be, runGet, Decoder(..), runGetIncremental,pushChunk, skip, bytesRead, isEmpty )
import qualified Network.Socket.ByteString as SBS
import Control.Monad.Trans.Maybe
import Data.Text.Encoding (encodeUtf8)
import Control.Monad.Trans.Class(lift)
import qualified Writer as W
import qualified Reader as R
import Control.Concurrent (forkIO)

import Control.Concurrent.Chan (writeChan, newChan, readChan)

type Port = String
data Cmd = Read BS.ByteString | Write BS.ByteString BS.ByteString


getReadCmd :: Get Cmd
getReadCmd = do
  keyLength <-fmap fromIntegral getWord16be
  key <- getByteString keyLength
  return $ Read key

getWriteCmd :: Get Cmd
getWriteCmd  = do
  keyLength <-  fmap fromIntegral getWord16be
  valueLength <- fmap fromIntegral getWord16be
  key <- getByteString keyLength
  value <- getByteString valueLength
  return $ Write key value


getCmd :: Get Cmd
getCmd = do
  cmd <- getWord8
  case cmd of
    1 -> getReadCmd
    2 -> getWriteCmd
    x -> fail $ "Unknown Command" <> show x



readByteString :: Socket -> MaybeT IO  BS.ByteString
readByteString socket = do
  bytes <- lift $ SBS.recv socket 1024
  if BS.length bytes == 0
    then MaybeT (return Nothing)
    else return bytes

readCmd :: Socket -> MaybeT IO  Cmd
readCmd socket = (readByteString socket) >>= start
  where start :: BS.ByteString -> MaybeT IO Cmd
        start bs = parse (runGetIncremental getCmd `pushChunk` bs)
        parse :: Decoder Cmd -> MaybeT IO Cmd
        parse (Done _ _ cmd) = return cmd
        parse (Fail _ _ _) = MaybeT (return Nothing)
        parse (Partial n) = (readByteString socket) >>= (parse . n . Just)




serverLoop :: W.WriterChannel -> R.ReaderChannel -> Socket -> IO ()
serverLoop writerChan readerChannel sock = do
  (clientSock, _) <- accept sock
  putStrLn "Incoming connection"
  forkIO $ handleConn writerChan readerChannel clientSock
  serverLoop writerChan readerChannel sock

executeCmd :: Socket -> W.WriterChannel -> R.ReaderChannel -> Cmd -> IO ()
executeCmd socket writerChan _ (Write key val)= do putStrLn "Execute Write"
                                                   writeChan writerChan (W.Write key val)
                                                   SBS.send socket $ encodeUtf8 "done"
                                                   return ()

executeCmd socket _ readerChan (Read key) = do putStrLn "Execute Read"
                                               resChan <- newChan
                                               writeChan readerChan (R.Read key , resChan)
                                               res <- readChan resChan
                                               case res of
                                                 (R.ReadResult Nothing) -> SBS.send socket (encodeUtf8 "Value not found")
                                                 (R.ReadResult (Just val)) -> SBS.send socket val
                                               return ()


handleConn :: W.WriterChannel -> R.ReaderChannel -> Socket -> IO ()
handleConn writerChan readerChan socket = do
  maybeCmd  <- runMaybeT (readCmd socket)
  case maybeCmd of
    Nothing -> (send socket "Invalid or missing Cmd") >> (putStrLn "Invalid Cmd")
    Just cmd -> executeCmd socket writerChan readerChan cmd
  sClose socket

start :: R.ReaderChannel -> W.WriterChannel -> Port -> IO ()
start readerChan writerChan port = do
  addrinfos <- getAddrInfo (Just defaultHints {addrFlags = [AI_PASSIVE]}) Nothing (Just port)
  let serverAddr = head addrinfos
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  bind sock (addrAddress serverAddr)
  listen sock 2
  putStrLn $ "Server is Listening on port:" <> port
  serverLoop writerChan readerChan sock
  sClose sock
