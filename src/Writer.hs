module Writer(start, WriterChannel, Cmd(..)) where

import Control.Monad (forever)
import qualified Log as Log
import Control.Concurrent
import Control.Concurrent.Chan


data Cmd = Write Log.Key Log.Value


type WriterChannel = Chan Cmd


execute :: Cmd -> IO ()
execute (Write key value) = Log.append key value

writer :: WriterChannel -> IO ()
writer chan = forever $ do cmd <- readChan chan
                           execute cmd


start :: IO WriterChannel
start = do
  chan <- newChan
  forkIO $ writer chan
  return chan
