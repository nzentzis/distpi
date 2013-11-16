{-# LANGUAGE RankNTypes, DeriveDataTypeable #-}
import Data.List
import Data.Maybe
import Data.Either
import Data.Typeable
import qualified Data.Conduit as C
import qualified Data.Conduit.Binary as CB

import Text.Printf

import Math.Pi

import Concurrency.Pool
import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class

import Network.Protocol
import Network

import Debug.Trace

controller :: (HostName, PortID)
controller = ("localhost", PortNumber 7777)

forceList :: (Eq a, Num a) => [a] -> [a]
forceList xs = (length $ filter (== 0) xs) `seq` xs

worker :: WorkUnit -> WorkResult 
worker unit@(WorkUnit start end) = WorkResult unit $ forceList digits
	where	digits = map bbp [start,start+1..end-1]

data ProcessingException = ConnectionLost | InvalidHandshake |
	ServerTerminated deriving (Show, Typeable)
instance Exception ProcessingException

process :: Int -> WorkPool WorkUnit WorkResult -> TQueue Message -> TQueue Message -> IO ()
process cores pool inp out = handshake >> (forever $ do
	evt <- atomically $ orElse (liftM Right $ readTQueue inp) (liftM Left $ getResult pool)
	case evt of
		Left r@(WorkResult (WorkUnit start end) _)	-> do
			atomically $ writeTQueue out $ WorkFinished r
			putStrLn $ printf "Finished workblock (%d-%d)" start end
		Right m	-> case m of
			SupplyWork u@(WorkUnit start end)	-> do
				putStrLn $ printf "Got workblock (%d-%d)" start end
				queueWork pool u
			Terminate -> liftIO $ throwIO ServerTerminated)
	where	handshake :: IO ()
		handshake = do
			atomically $ writeTQueue out $ ClientHandshake (SystemInfo cores)
			response <- atomically $ readTQueue inp
			case response of
				ServerHandshake work	-> mapM_ (queueWork pool) work
				_			-> throwIO $ InvalidHandshake

queueSink :: (MonadIO m, Show a) => TQueue a -> C.Consumer a m ()
queueSink q = forever $ do
	msg <- C.await
	case msg of
		Nothing	-> return ()
		Just x	-> liftIO $ do
			atomically $ writeTQueue q x

queueSource :: (MonadIO m, Show a) => TQueue a -> C.Producer m a
queueSource q = forever $ (liftIO $ do
	elem <- atomically $ readTQueue q
	return elem
	) >>= C.yield

main = withSocketsDo $ do
	cores <- numCores
	pool <- buildPool cores worker
	putStrLn $ printf "Created pool with %d workers" cores
	hdl <- uncurry connectTo controller

	-- Create the two comms threads
	(inqueue,outqueue) <- atomically $ liftM2 (,) newTQueue newTQueue
	baseThread <- myThreadId
	reader <- forkFinally ((CB.sourceHandle hdl) C.$= messageDecoder C.$$ queueSink inqueue)
		(\e->case e of
			Left _	-> throwTo baseThread ConnectionLost
			_	-> return ())
	writer <- forkFinally (queueSource outqueue C.$= messageEncoder C.$$ (CB.sinkHandle hdl))
		(\e->case e of
			Left _	-> throwTo baseThread ConnectionLost
			_	-> return ())

	-- And process queue elements forever
	catch (process cores pool inqueue outqueue) (\e->case e :: ProcessingException of
		ConnectionLost		-> putStrLn "Lost connection to server"
		InvalidHandshake	-> putStrLn "Handshaking failed"
		ServerTerminated	-> putStrLn "Server requested shutdown")
	catch (mask_ $ killThread writer >> killThread reader)
		((\_->return ()) :: SomeException -> IO ())

