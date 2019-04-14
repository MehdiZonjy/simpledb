module Writer(start, WriterChannel, Cmd(..)) where

import Control.Monad (forever)
import qualified Log as Log
import Control.Concurrent
import Control.Concurrent.Chan
import qualified Reader as Reader


data Cmd = Write Log.Key Log.Value
type WriterChannel = Chan Cmd


execute :: Cmd -> IO (Log.Key, Log.ValueDescriptor)
execute (Write key value) = Log.append key value

writer :: Reader.ReaderChannel -> WriterChannel -> IO ()
writer readerChan writerChan = forever $ do cmd <- readChan writerChan
                                            (key, valDesc) <- execute cmd
                                            resChan <- newChan
                                            writeChan readerChan (Reader.Update key valDesc, resChan)
                                            res <- readChan resChan
                                            putStrLn (show res)


start :: Reader.ReaderChannel -> IO WriterChannel
start readerChan = do
  chan <- newChan
  forkIO $ writer readerChan chan
  return chan
