module Reader(start, ReaderChannel, Cmd(..), Response(..)) where

import Control.Monad (forever)
import qualified Log as Log
import Control.Concurrent
import Control.Concurrent.Chan
import qualified Data.Text as T
import Control.Monad.Trans.Reader
import Control.Monad.IO.Class (liftIO)
import qualified Data.Map as M

data Cmd = Read Log.Key | Update Log.Key Log.ValueDescriptor
data Response = Ok | ReadResult (Maybe Log.Value) deriving Show

type ReaderChannel = Chan (Cmd, Chan Response)



execute :: ReaderChannel -> Chan Response ->  Cmd -> ReaderT Log.KeyDir IO ()
execute readerChan resChan (Read key) = do keyDir <- ask
                                           liftIO $ Log.get keyDir key >>= (return. ReadResult) >>= (writeChan resChan)
                                           loop readerChan
                                           
execute readerChan resChan (Update key valDesc) = do keyDir <- ask
                                                     liftIO $ writeChan resChan Ok
                                                     local (M.insert key valDesc) (loop readerChan)

loop :: ReaderChannel -> ReaderT Log.KeyDir IO ()
loop chan = do (cmd, resChan) <- liftIO $ readChan chan
               execute chan resChan cmd


start :: IO ReaderChannel
start = do
  keyDir <- Log.loadKeyDir
  chan <- newChan
  forkIO $ runReaderT (loop chan) keyDir
  return chan

