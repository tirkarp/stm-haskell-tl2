module Main where

import Criterion.Main

import Control.Concurrent
import Control.Monad
import Data.List(sort, delete)

import CigaretteSmokers
import MultipleBarbers
import SantaClaus
import SleepingBarber
import SushiBar1
import SushiBar2

import qualified LinkedList as LL
import PriorityQueue
import HeapPQ
import THeapPQ

-- import STM
import Control.Concurrent.STM


{-   Producer/Consumer checkers and properties   -}

forkNJoin :: [IO a] -> IO ()
forkNJoin ios = do
  children <- forM ios $ \io -> do
    child <- newEmptyMVar
    _ <- forkFinally io $ \_ -> putMVar child ()
    return child
  forM_ children $ \child -> takeMVar child


prodNCons :: (Ord a, Show a, PriorityQueue q) =>
    Int -> Int -> q a a -> [a] -> IO ()
prodNCons pcount ccount pq vals = do
  prodVals <- newMVar vals
  consVals <- newMVar vals
  let prods = replicate pcount $ prodRole prodVals
  let conss = replicate ccount $ consRole prodVals consVals
  forkNJoin $ prods ++ conss
  where
      prodRole prodVals = do
        tid <- myThreadId
        vs <- takeMVar prodVals
        case vs of
          [] -> do
            putMVar prodVals []
          (v:vs') -> do
            putMVar prodVals vs'
            atomically $ insert pq v v
            prodRole prodVals

      consRole prodVals consVals = do
        tid <- myThreadId
        mx <- atomically $ tryDeleteMin pq
        case mx of
          Nothing -> do
            vs <- takeMVar prodVals
            putMVar prodVals vs
            case vs of
              [] -> do
                return ()
              _  -> consRole prodVals consVals
          (Just x) -> do
            vs <- takeMVar consVals
            case delete x vs of
              [] -> do
                putMVar consVals []
              vs' -> do
                putMVar consVals vs'
                consRole prodVals consVals


prodNconsK :: (Ord a, Show a, PriorityQueue q) => STM (q a a) -> Int -> Int -> [a] -> IO ()
prodNconsK pqcons n k vals = do
  pq <- atomically pqcons
  prodNCons n k pq vals




{-   Per implementation test runner   -}

coarseHeap :: PriorityQueue q => String -> STM (q Int Int) -> IO ()
coarseHeap base cons =  do
      prodNconsK cons 16 16 [1..100000]


fineHeap :: PriorityQueue q => String -> STM (q Int Int) -> IO ()
fineHeap base cons =  do
      prodNconsK cons 16 16 [1..100000]


performCoarseTest :: Int -> IO ()
performCoarseTest x = do
    putStrLn "Coarse-grain STM binary heap: Start"
    coarseHeap "heap-pq" (new :: STM (HeapPQ Int Int))
    putStrLn "Coarse-grain STM binary heap: End"


performFineTest :: Int -> IO ()
performFineTest x = do
    putStrLn "Fine-grain STM binary heap: Start"
    fineHeap "theap-pq" (new :: STM (THeapPQ Int Int))
    putStrLn "Fine-grain STM binary heap: End"



main :: IO ()
main = defaultMain [bgroup "B-heap" [bench "coarse-grained"  (whnf performCoarseTest 0), bench "fine-grained" (whnf performFineTest 0)]]