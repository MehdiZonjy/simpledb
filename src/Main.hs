module Main where
import qualified Log as Log
import qualified Writer as Writer



data Config = Config {
    keyDir :: Log.KeyDir
  }




main :: IO ()
main = do
  Writer.start "8181"
--  kd <- Log.loadKeyDir
--  let env = Config kd
--  putStrLn "Hello World"