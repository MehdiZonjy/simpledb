module Main where
import qualified Log as Log
import qualified Writer as Writer
import qualified Server as Server
import qualified Reader as Reader

data Config = Config {
    keyDir :: Log.KeyDir
  }




main :: IO ()
main = do
  readerChan <- Reader.start
  writerChan <- Writer.start readerChan
  Server.start readerChan writerChan "8181"
