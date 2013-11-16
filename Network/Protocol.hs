module Network.Protocol where

import Text.Printf

import Data.Maybe
import Data.Serialize
import qualified Data.Conduit as C
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL
import qualified Data.ByteString as B

import Control.DeepSeq
import Control.Monad

data WorkUnit = WorkUnit Int Int deriving (Eq,Show)
data WorkResult = WorkResult WorkUnit [Int] deriving (Show,Eq)
data SystemInfo = SystemInfo Int deriving (Eq,Show)

data Message = ClientHandshake SystemInfo | ServerHandshake [WorkUnit] |
	StartedWork WorkUnit | WorkFinished WorkResult | SupplyWork WorkUnit |
	Terminate deriving (Eq,Show)

instance Serialize SystemInfo where
	get = liftM (SystemInfo . fromIntegral) getWord32be
	put (SystemInfo n) = putWord32be $ fromIntegral n

instance Serialize WorkUnit where
	get = liftM2 (\a b->WorkUnit (fromIntegral a) (fromIntegral b)) getWord64be getWord64be
	put (WorkUnit a b) = putWord64be (fromIntegral a) >> putWord64be (fromIntegral b)

workUnitDigits :: WorkUnit -> Int
workUnitDigits (WorkUnit start end) = end-start

instance Serialize WorkResult where
	get = do
		unit <- get
		digits <- replicateM (workUnitDigits unit) (liftM fromIntegral getWord8)
		return $ WorkResult unit digits
	put (WorkResult u@(WorkUnit start end) ds) = if (end-start) /= length ds then
		fail "Insufficient result items to represent full packet" else
		put u >> mapM_ (putWord8 . fromIntegral) ds

instance NFData WorkUnit where
	rnf (WorkUnit a b) = (rnf a) `seq` (rnf b)

instance NFData WorkResult where
	rnf (WorkResult u ds) = (rnf u) `seq` (rnf ds)

instance Serialize Message where
	get = do
		mtype <- getWord8
		case mtype of
			0x00	-> liftM ClientHandshake get
			0x01	-> do
				units <- liftM fromIntegral getWord8
				liftM ServerHandshake $ replicateM units get
			0x02	-> liftM StartedWork get
			0x03	-> liftM WorkFinished get
			0x04	-> liftM SupplyWork get
			0x05	-> return Terminate
			x	-> fail ("Unable to decode result - incomprehensible mtype: " ++ show x)
	put (ClientHandshake i) = putWord8 0x00 >> put i
	put (ServerHandshake i) = putWord8 0x01 >>
		(putWord8 $ fromIntegral $ length i) >>
		mapM_ put i
	put (StartedWork u) = putWord8 0x02 >> put u
	put (WorkFinished r) = putWord8 0x03 >> put r
	put (SupplyWork u) = putWord8 0x04 >> put u
	put Terminate = putWord8 0x05

-- Serialization stuff
messageEncoder :: Monad m => C.Conduit Message m B.ByteString
messageEncoder = forever $ C.await >>= (\x->case x of
	Nothing	-> return ()
	Just m	-> C.yield $ encode m)

messageDecoder :: Monad m => C.Conduit B.ByteString m Message
messageDecoder = forever (decodeOne Nothing)
	where	decodeOne Nothing = do
			bs <- C.await
			if not $ isJust bs then return () else do
				let res = runGetPartial get $ fromJust bs
				case res of
					Fail e rem	-> C.leftover rem
					Done res rem	-> C.leftover rem >> C.yield res
					x		-> decodeOne $ Just x
		decodeOne (Just part) = do
			bs <- C.await
			if not $ isJust bs then return () else do
				let res = runGetPartial get $ fromJust bs
				case res of
					Fail e rem	-> fail e >> C.leftover rem
					Done res rem	-> C.leftover rem >> C.yield res
					x		-> decodeOne $ Just x
