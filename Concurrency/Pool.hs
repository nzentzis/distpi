module Concurrency.Pool (
	WorkPool,
	buildPool,
	terminatePool,
	queueWork,
	getResult,
	tryGetResult,
	getResults,
	numCores
	) where

import Control.Concurrent.STM
import Control.Concurrent
import Control.Monad
import Control.DeepSeq

data NFData o => Worker i o = Worker {
	wkOutput :: TMVar o,
	wkInput :: TMVar i
	}

data NFData o => WorkPool i o = WorkPool {
	workers :: [(ThreadId, Worker i o)],
	total :: Int
	}

buildWorker :: NFData o => (i -> o) -> Worker i o -> IO ()
buildWorker f w = forever $ liftM f fetchWorkUnit >>= putWorkResult
	where	fetchWorkUnit = atomically $ takeTMVar $ wkInput w
		putWorkResult = atomically . putTMVar (wkOutput w) . force

buildPool :: NFData o =>  Int -> (i -> o) -> IO (WorkPool i o)
buildPool nWork f = do
	workers <- atomically $ replicateM nWork (liftM2 Worker newEmptyTMVar newEmptyTMVar)
	workThreads <- mapM (forkIO . buildWorker f) workers
	return $ WorkPool (zip workThreads workers) nWork

terminatePool :: NFData o => WorkPool i o -> IO ()
terminatePool = mapM_ (killThread . fst) . workers

queueWork :: NFData o => WorkPool i o -> i -> IO ()
queueWork pool workItem = atomically $ anyOf (map (putWork workItem . snd) $ workers pool)
	where	putWork x worker = putTMVar (wkInput worker) x

-- Tries all actions in series, returning the first one that does not retry. If
-- all actions retry, the entire anyOf retries.
anyOf :: [STM a] -> STM a
anyOf (x:[]) = x
anyOf (x:xs) = orElse x $ anyOf xs

getResult :: NFData o => WorkPool i o -> STM o
getResult pool = anyOf (map (getWorkRes . snd) $ workers pool)
	where	getWorkRes = takeTMVar . wkOutput

tryGetResult :: NFData o => WorkPool i o -> STM (Maybe o)
tryGetResult pool = orElse (liftM Just $ anyOf getWorks) (return Nothing)
	where	getWorkRes = takeTMVar . wkOutput
		getWorks = map (getWorkRes . snd) $ workers pool

getResults :: NFData o => WorkPool i o -> STM [o]
getResults pool = do
	res <- liftM concat $ mapM (\(_,w)->tryTakeTMVar (wkOutput w) >>= (\x->case x of
		Nothing	-> return []
		Just r	-> return [r])) (workers pool)
	if null res then retry else return res

numCores :: IO Int
numCores = getNumCapabilities
