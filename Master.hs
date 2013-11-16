{-# LANGUAGE RankNTypes #-}
import Data.Char
import qualified Data.Conduit as C
import qualified Data.Conduit.Binary as CB
import qualified Data.ByteString as B

import Text.Printf

import Network.Protocol
import Network

import Control.Concurrent.STM
import Control.Concurrent
import Control.Monad.IO.Class
import Control.Monad

import GHC.IO.Handle (Handle)

blockSize = 512

data ClientInfo = ClientInfo {
	clHdl :: Handle,
	clHost :: HostName,
	clActiveWork :: TVar [WorkUnit],
	clCores :: Int
	}

data ServerState = ServerState {
	completedBlocks :: TQueue WorkResult,
	clients :: TVar [ClientInfo],
	nextBlock :: TVar WorkUnit
	}

advanceBlock :: ServerState -> STM ()
advanceBlock sv = modifyTVar' (nextBlock sv)
	(\(WorkUnit start end)->WorkUnit (start+blockSize) (end+blockSize))

getBlock :: ServerState -> STM WorkUnit
getBlock sv = readTVar (nextBlock sv) >>= (\res->advanceBlock sv >> return res)

getBlocks :: ServerState -> Int -> STM [WorkUnit]
getBlocks sv n = replicateM n (getBlock sv)

addClient :: ServerState -> ClientInfo -> STM ()
addClient sv inf = modifyTVar' (clients sv) (inf :)

removeClient :: ServerState -> ClientInfo -> STM ()
removeClient sv inf = modifyTVar' (clients sv) $ filter (\x->clHost x /= clHost inf)

-- Handle a single client
handleClient :: ServerState -> (Handle, HostName) -> IO ()
handleClient sv (hdl, host) = putStrLn ("Connection recieved from " ++ host) >>
	((CB.sourceHandle hdl) C.$= messageDecoder C.=$= handler C.=$=
	messageEncoder C.$$ (CB.sinkHandle hdl))
	where	handler :: C.Conduit Message IO Message
		handler = performHandshake >> (forever $ do
				msg <- C.await
				case msg of
					Nothing	-> return ()
					Just m	-> handleMessage m)
		handleMessage :: Message -> C.Conduit Message IO Message
		handleMessage (WorkFinished r) = (liftIO $ do
			let (WorkResult (WorkUnit start end) _) = r
			putStrLn $ printf "Client %s finished workblock (%d-%d)" host start end
			atomically $ do
				writeTQueue (completedBlocks sv) r
				if end > 2000 then return Terminate else
					liftM SupplyWork (getBlock sv)
			) >>= C.yield
		performHandshake :: C.Conduit Message IO Message
		performHandshake = do
			msg <- C.await
			case msg of
				Nothing -> fail "Cannot handshake"
				Just m -> case m of
					(ClientHandshake (SystemInfo cores)) -> (liftIO $ atomically $ do
						-- Build work
						work <- getBlocks sv cores

						-- Create and install client info
						workVar <- newTVar work
						let ci = ClientInfo hdl host workVar cores
						addClient sv ci
						return $ ServerHandshake work) >>= C.yield
					x -> fail ("Invalid first packet: " ++ show x)

queueSource :: MonadIO m => TQueue a -> C.Producer m a
queueSource q = forever $ (liftIO $ atomically $ readTQueue q) >>= C.yield

-- Print all incoming digits
printBlocks :: ServerState -> IO ()
printBlocks sv = C.runResourceT $ (queueSource $ completedBlocks sv) C.=$= converter C.$$ (CB.sinkFile "digits")
	where	converter :: Monad m => C.Conduit WorkResult m B.ByteString
		converter = forever $ do
			res <- C.await
			case res of
				Nothing	-> return ()
				Just r -> C.yield $ printLine r
		printLine :: WorkResult -> B.ByteString
		printLine (WorkResult (WorkUnit start end) xs) = encodeStr $
			unlines $ map (\(x,v)->printf "%d:%c" x $ undigit v) $
			zip [start,start+1..end-1] xs
		undigit :: Int -> Char
		undigit = ("0123456789abcdef" !!)
		encodeStr :: String -> B.ByteString
		encodeStr = B.pack . map (fromIntegral . ord)

main = withSocketsDo $ do
	sck <- listenOn $ PortNumber 7777
	serverState <- atomically $ liftM3 ServerState
		newTQueue (newTVar []) (newTVar (WorkUnit 0 blockSize))
	printer <- forkIO $ printBlocks serverState
	forever $ accept sck >>=
		(forkIO . handleClient serverState . (\(a,b,c)->(a,b)))
