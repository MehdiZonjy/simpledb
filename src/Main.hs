module Main where
import qualified Log as Log
import qualified Writer as Writer
import qualified Server as Server


data Config = Config {
    keyDir :: Log.KeyDir
  }




main :: IO ()
main = do
  writerChan <- Writer.start
  Server.start writerChan "8181"
--  kd <- Log.loadKeyDir
--  let env = Config kd
--  putStrLn "Hello World"