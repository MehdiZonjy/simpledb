module Server where

import Network.Socket
import qualified Data.ByteString as BS
import Data.Binary.Get (Get ,getByteString, getWord16be, runGet, Decoder(..), runGetIncremental,pushChunk, skip, bytesRead, isEmpty )
import qualified Network.Socket.ByteString as SBS
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Class(lift)
import Writer (WriterChannel, Cmd(..))
import Control.Concurrent (forkIO)
import Control.Concurrent.Chan (writeChan)

type Port = String


getWriteCmd :: Get Cmd
getWriteCmd  = do
  keyLength <-fmap fromIntegral getWord16be
  valueLength <- fmap fromIntegral getWord16be
  key <- getByteString keyLength
  value <-  getByteString valueLength
  return $ Write key value


readByteString :: Socket -> MaybeT IO  BS.ByteString
readByteString socket = do
  bytes <- lift $ SBS.recv socket 1024
  if BS.length bytes == 0
    then MaybeT (return Nothing)
    else return bytes

readWriteCmd :: Socket -> MaybeT IO  Cmd
readWriteCmd socket = (readByteString socket) >>= start
  where start :: BS.ByteString -> MaybeT IO Cmd
        start bs = parse (runGetIncremental getWriteCmd `pushChunk` bs)
        parse :: Decoder Cmd -> MaybeT IO Cmd
        parse (Done _ _ cmd) = return cmd
        parse (Fail _ _ _) = MaybeT (return Nothing)
        parse (Partial n) = (readByteString socket) >>= (parse . n . Just)




serverLoop :: WriterChannel -> Socket -> IO ()
serverLoop writerChan sock = do
  (clientSock, _) <- accept sock
  putStrLn "Incoming connection"
  forkIO $ handleConn writerChan clientSock
  serverLoop writerChan sock


handleConn :: WriterChannel -> Socket -> IO ()
handleConn writerChan socket = do
  maybeCmd  <- runMaybeT (readWriteCmd socket)
  case maybeCmd of
    Nothing -> (send socket "Invalid or missing Cmd") >> (putStrLn "error")
    Just cmd ->  writeChan writerChan cmd -- (Log.append key value) >> (putStrLn "key value added") >> (send socket "ok" ) >> (return ())
  sClose socket

start :: WriterChannel -> Port -> IO ()
start writerChan port = do
  addrinfos <- getAddrInfo (Just defaultHints {addrFlags = [AI_PASSIVE]}) Nothing (Just port)
  let serverAddr = head addrinfos
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  bind sock (addrAddress serverAddr)
  listen sock 2
  putStrLn $ "Server is Listening on port:" <> port
  serverLoop writerChan sock
  sClose sock
