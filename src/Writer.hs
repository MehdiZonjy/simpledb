module Writer(start) where

import Network.Socket
import qualified Data.ByteString as BS
import Data.Binary.Get (Get ,getByteString, getWord16be, runGet, Decoder(..), runGetIncremental,pushChunk, skip, bytesRead, isEmpty )
import qualified Network.Socket.ByteString as SBS
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Class(lift)
import qualified Log as Log
type Port = String


data WriteCmd = WriteCmd {
    cmdKey :: BS.ByteString
  , cmdValue :: BS.ByteString
  }

getWriteCmd :: Get WriteCmd
getWriteCmd  = do
  keyLength <-fmap fromIntegral getWord16be
  valueLength <- fmap fromIntegral getWord16be
  key <- getByteString keyLength
  value <-  getByteString valueLength
  return $ WriteCmd key value


readByteString :: Socket -> MaybeT IO  BS.ByteString
readByteString socket = do
  bytes <- lift $ SBS.recv socket 1024
  if BS.length bytes == 0
    then MaybeT (return Nothing)
    else return bytes

readWriteCmd :: Socket -> MaybeT IO  WriteCmd
readWriteCmd socket = (readByteString socket) >>= start
  where start :: BS.ByteString -> MaybeT IO WriteCmd
        start bs = parse (runGetIncremental getWriteCmd `pushChunk` bs)
        parse :: Decoder WriteCmd -> MaybeT IO WriteCmd
        parse (Done _ _ cmd) = return cmd
        parse (Fail _ _ _) = MaybeT (return Nothing)
        parse (Partial n) = (readByteString socket) >>= (parse . n . Just)




writerLoop :: Socket -> IO ()
writerLoop sock = do
  (clientSock, _) <- accept sock
  putStrLn "Incoming connection"
  handleConn clientSock
  writerLoop sock


handleConn :: Socket -> IO ()
handleConn socket = do
  maybeCmd  <- runMaybeT (readWriteCmd socket)
  case maybeCmd of
    Nothing -> (send socket "Invalid or missing Cmd") >> (putStrLn "error")
    Just (WriteCmd key value) -> (Log.append key value) >> (putStrLn "key value added") >> (send socket "ok" ) >> (return ())
  sClose socket

start :: Port -> IO ()
start port = do
  addrinfos <- getAddrInfo (Just defaultHints {addrFlags = [AI_PASSIVE]}) Nothing (Just port)
  let serverAddr = head addrinfos
  sock <- socket (addrFamily serverAddr) Stream defaultProtocol
  bind sock (addrAddress serverAddr)
  listen sock 2
  putStrLn "Writer is Listening"
  writerLoop sock
  sClose sock