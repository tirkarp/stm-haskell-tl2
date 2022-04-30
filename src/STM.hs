module STM where

--import GHC.Conc(myTId)
import Data.IORef
import Control.Concurrent
--import Data.HashTable
import Control.Concurrent.MVar
import System.IO.Unsafe
import Foreign.StablePtr
    ( newStablePtr,
      castPtrToStablePtr,
      castStablePtrToPtr,
      deRefStablePtr,
      freeStablePtr )
import System.Mem.StableName
import Foreign.Ptr
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Map as Map

import Control.Applicative
import Control.Monad (liftM, ap)


--assert :: Bool -> String -> IO ()
--assert False s = error s
--assert True s  = return ()

data STM a = STM (TState -> IO (TResult a))

data TResult a = Valid TState a | Retry TState | Invalid TState

instance Functor STM where
  fmap = liftM

instance Applicative STM where
  pure  = return
  (<*>) = ap

instance Monad STM where
	(STM t1) >>= f = STM(\tState -> do
				tRes <- t1 tState				
				case tRes of
					Valid nTState v ->
						let (STM t2) = f v in 
							t2 nTState
					Retry nTState 	-> return (Retry nTState)
					Invalid  nTState-> return (Invalid nTState)
				)

	return x = STM (\tState -> return (Valid tState x))


retry :: STM a
retry = STM $ \tState -> return (Retry tState)

orElse :: STM a -> STM a -> STM a
orElse (STM t1) (STM t2) = STM $ \tstate -> do
	tsCopy <- cloneTState tstate
	tRes1 <- t1 tstate
	case tRes1 of
		Retry nTState1 	-> do
				tRes2 <- t2 tsCopy
				case tRes2 of
					Retry nTState2 -> do	fTState <- mergeTStates nTState2 nTState1 
								return (Retry fTState)
					Valid nTState2 r ->  do	fTState <- mergeTStates nTState2 nTState1
								return (Valid fTState r)
					_ ->         return tRes2
		_	-> return tRes1

cloneTState :: TState -> IO TState
cloneTState tstate = do
	nws <- cloneWS (writeSet tstate)
	rs <- readIORef (readSet tstate)
	let nrss = Set.fromList (Set.toList rs)
	nrs <- newIORef nrss
	return (Tr (transId tstate) (readStamp tstate) nrs nws)


cloneWS :: WriteSet -> IO WriteSet
cloneWS rws = do
	ws <- readIORef rws
	let nws = Map.fromList (Map.toList ws)
	newIORef nws

mergeTStates :: TState -> TState -> IO TState
mergeTStates ts1 ts2 = do
	rs1 <- readIORef (readSet ts1)
	rs2 <- readIORef (readSet ts2)
	let nrs = Set.union rs1 rs2
	writeIORef (readSet ts1) nrs
	return ts1
	
		


unreadTVar :: TVar a -> STM ()
unreadTVar tvar@(TVar _ id w c _) = STM  $ \tstate -> do
				found <- lookUpWS (writeSet tstate) id
				case found of
				  Nothing -> return (Valid tstate ())
				  Just _ -> do  deleteRS (readSet tstate) id
						return (Valid tstate ())

data TState = Tr {
   transId :: TId,
   readStamp :: Integer,
   readSet :: ReadSet,
   writeSet :: WriteSet
}

type TId = Integer

type Lock = IORef Integer

myTId :: IO TId
myTId = do
	x <- myThreadId
	return (((2*). read . drop 9) (show x))



data TVar a = TVar{
	lock :: Lock,
	id :: Integer,
	writeStamp :: IORef Integer,
	content :: IORef a,
        waitQueue :: IORef [MVar ()]
}

newTVarIO :: a -> IO (TVar a)
newTVarIO a = do
		lock <- newLock
		ws <- readLock lock
		id <- newID
		--assert (odd ws) "newtvar: value in lock is not odd!"
		writeStamp <- newIORef ws
		content <- newIORef a
		waitQueue <- newIORef []
		return (TVar lock id writeStamp content waitQueue)


instance Eq (TVar a) where
	(TVar l1 _ _ _ _) == (TVar l2 _ _ _ _) = l1 == l2


newLock :: IO Lock
newLock = newIORef 1

--newID2 :: Lock -> IO Integer
--newID2 lock = do stn <- makeStableName lock
--	        return (hashStableName stn)


idref :: IORef Integer
idref = unsafePerformIO (newIORef 0)

newID :: IO Integer
newID = do
	--print "increment"
	cur <- readIORef idref
	changed <- atomCAS idref cur (cur+1)
	if changed then return (cur+1) else newID
	

newTVar :: a -> STM (TVar a)
newTVar a = STM $ \tState -> do
			tvar <- newTVarIO a		
			return (Valid tState tvar)





newTState :: IO TState
newTState = do
	tid <- myTId
	--assert (even tid) "Transaction id is not even!"
	readStamp <- readIORef globalClock --incrementGlobalClock
	--assert (odd readStamp) "newtstate: globalclock not odd!"
	readSet <- newIORef (Set.empty)
	writeSet <- newIORef (Map.empty)
	--writeSet <- new (==) (\x -> hashInt (fromInteger x))
        return (Tr tid readStamp readSet writeSet)

atomically :: STM a -> IO a
atomically stmac@(STM ac) = do
--	print "ATOMICALY"
	ts <- newTState
	r <- ac ts
	case r of
		Invalid nts ->do 	--cleanWriteSet nts  
                                        clean nts
					atomically stmac
		Retry nts   -> do
				rs <- readIORef (readSet nts)
				let lrs = Set.toList rs
				(valid,locks) <- validateAndAcquireLocks (readStamp  nts) (transId nts) lrs
				case valid of
					False -> do unlock (transId nts) locks
						    clean nts
						    atomically stmac
			
                                        True -> do 
						waitMVar <- newEmptyMVar
						addToWaitQueues waitMVar lrs
						--clean nts
						unlock (transId nts) locks
						clean nts
						takeMVar waitMVar
						atomically stmac
		Valid nts a -> do
				ti <- myTId
				--assert (ti == (transId nts)) "theadId != transId"
				wslist <- toListWS (writeSet nts)
				tup <- getLocks (transId nts) wslist
				case tup of
					(False,locks) -> do
						--cleanWriteSet nts
						--ti <- myTId
						unlock (transId nts) locks
						clean nts
						atomically stmac
					(True,locks) -> do
						wstamp <- incrementGlobalClock
				--		ti <- myTId
						valid <- validateReadSet (readSet nts) (readStamp nts) (transId nts)
						if valid
							then do
								commitChangesToMemory wstamp wslist
								--cleanWriteSet nts
								wakeUpBQ wslist
								--clean nts
								unlock (transId nts) locks
								clean nts
								return a
							else do
								--cleanWriteSet nts
								--clean nts
								unlock (transId nts) locks
								clean nts
								atomically stmac

clean nts = do 	wslist <- toListWS (writeSet nts)
		mapM_ (\(_,ptr) -> freeStablePtr (castPtrToStablePtr ptr)) wslist 		


unblockThreads :: (Integer,Ptr()) -> IO ()
unblockThreads (i,v) = do 
	(WSE _ _ _ _ queue) <- castFromPtr v 
	listMVars <- readIORef queue
	mapM_ (\mvar -> tryPutMVar mvar ()) listMVars
	writeIORef queue []

wakeUpBQ :: [(Integer,Ptr())]  -> IO ()
wakeUpBQ = mapM_ unblockThreads


addToWaitQueues :: MVar () -> [RSEntry] -> IO ()
addToWaitQueues mvar  = mapM_ (\( RSE _ lock _ iomlist) -> do
					--assert (isLockedIO lock) ("AddtoQueues: tvar not locked!!!")
					list <- readIORef iomlist
					writeIORef iomlist (mvar:list)) 

validateAndAcquireLocks :: Integer -> Integer -> [RSEntry] -> IO (Bool,[(IORef Integer,Lock)])
validateAndAcquireLocks readStamp myId readSet = validateRSL2 readStamp myId [] readSet


validateRSL2 :: Integer -> Integer  -> [(IORef Integer,Lock)]  -> [RSEntry] -> IO (Bool,[(IORef Integer,Lock)])
validateRSL2  readStamp myId locks [] = return (True, locks)
validateRSL2  readStamp myId locks ((RSE _ lock wstamp _):ls) = do
		lockValue <- readLock lock
		case (isLocked lockValue) of
			True -> do --assert (lockValue /= myId) "validate and lock readset: already locked by me!!!"
				   return (False,locks) 
			False -> do if (lockValue > readStamp)
				 	then return (False,locks)
					else do r <- atomCAS lock lockValue myId
						case r of
							True  -> validateRSL2 readStamp myId ((wstamp,lock):locks) ls
							False -> return (False,locks)

getLocks :: TId -> [(Integer,Ptr())] -> IO (Bool,[(IORef Integer, Lock)])
getLocks tid ws = getLocks2 tid ws []



getLocks2 :: TId -> [(Integer,Ptr())] -> [(IORef Integer,Lock)] -> IO (Bool,[(IORef Integer,Lock)])
getLocks2 ti [] l = return (True,l)
getLocks2 tid ((_,ptr):xs) locks = do
	(WSE lock iowstamp _ _ _) <- castFromPtr ptr
	lockValue <- readLock lock
	if (isLocked lockValue)
		then do
			--assert (lockValue /= tid) "Locking WS: lock already held by me!!"
			return (False,locks)
		else do
			r <- atomCAS lock lockValue tid
			case r of
				True  -> do 	getLocks2 tid xs ((iowstamp,lock):locks)
				False -> do 	return (False,locks)

commitChangesToMemory :: Integer -> [(Integer,Ptr())] -> IO ()
commitChangesToMemory wstamp wset = mapM_ (commit wstamp) wset
	where
		commit :: Integer -> (Integer,Ptr())-> IO ()
		commit wstamp (id,ptr) = do
			(WSE _ iowstamp iocontent v _) <- castFromPtr ptr
			writeIORef iowstamp wstamp 
			writeIORef iocontent v



unlock :: Integer -> [(IORef Integer,Lock)] -> IO ()
unlock tid = mapM_ (\(iows,lock) -> do 	ws <- readIORef iows
					unlocked<- atomCAS lock tid ws
					--return ()
					--assert unlocked "COULD NOT UNLOCK LOCK"
					return ())
			           	


isLocked :: Integer -> Bool
isLocked = even

isLockedIO :: Lock -> Bool
isLockedIO lock = unsafePerformIO $ do
					v<- readIORef lock
					return (isLocked v)

-- isLocked :: Lock -> IO Bool
--isLocked lock = do
--	v <- readIORef lock
--	return (v/=(-1))

readLock :: Lock -> IO Integer
readLock = readIORef

globalClock :: IORef Integer
globalClock = unsafePerformIO (newIORef 1)

atomCAS :: Eq a => IORef a -> a -> a -> IO Bool
atomCAS ptr old new = atomicModifyIORef ptr (\cur -> if cur == old then (new, True) else (cur,False))

incrementGlobalClock :: IO Integer
incrementGlobalClock = do
	ov <- readIORef globalClock
	changed <- atomCAS globalClock ov (ov+2)
	if changed then do --assert (odd (ov+2)) "Clock is not an odd number!"
			   return (ov+2) 
		   else incrementGlobalClock
	

----------------------------------------------
----------- Read and Write Sets -----------------

type ReadSet = IORef (Set RSEntry)   --IORef [(Lock, IORef Int)]

data RSEntry = RSE Integer Lock (IORef Integer) (IORef [MVar ()])

instance Eq RSEntry where
	(RSE i1 _ _ _) == (RSE i2 _ _ _) = i1 == i2

instance Ord RSEntry where
	compare (RSE i1 _ _ _) (RSE i2 _ _ _)
		| i1 == i2    	=  EQ
         	| i1 <= i2    	=  LT
         	| otherwise 	=  GT


--putRS2 :: ReadSet -> (Lock, IORef Int) -> ReadSet
--putRS2 x v = v:x

newRSE :: Integer -> IO RSEntry
newRSE id  = do
		writeStamp <- newIORef 0
		waitQueue <- newIORef []
		return (RSE id writeStamp writeStamp waitQueue)
--(RSE id lock iorefwstamp queue)

putRS :: ReadSet -> RSEntry-> IO ()
putRS ioReadSet value = do 
	readLog <- readIORef ioReadSet		
	writeIORef ioReadSet (Set.insert value readLog)

deleteRS :: ReadSet -> Integer -> IO ()
deleteRS iors id = do
	rs <- readIORef iors
	rse <- newRSE id
	let newrs = Set.delete rse rs
	writeIORef iors newrs 

type WriteSet = IORef (Map.Map Integer (Ptr()))

data WSEntry a = WSE Lock (IORef Integer) (IORef a) a (IORef [MVar ()])

putWS :: WriteSet -> Integer -> Ptr() -> IO ()
putWS rws ti v = do 
		ws <-readIORef rws
		let (old,newws) = Map.insertLookupWithKey (\key new_value old_value -> new_value) ti v ws
		writeIORef rws newws
                case old of
			Just ptr -> do 	freeStablePtr (castPtrToStablePtr ptr)
					return ()
			Nothing -> 	return ()

lookUpWS :: WriteSet -> Integer -> IO (Maybe (Ptr ()))
lookUpWS rwset id = do  ws <- readIORef rwset
		        let v = Map.lookup id ws
			return v

toListWS :: WriteSet -> IO [(Integer,Ptr())]
toListWS rwset = do	ws <- readIORef rwset
		    	let v = Map.toList ws
			return v

--fromListWS :: [(Integer,Ptr())] -> IO WriteSet
--fromListWS l = do	let v = Map.fromList l
--			newIORef v

validateReadSet ::  ReadSet -> Integer -> Integer-> IO Bool
validateReadSet ioReadSet readStamp myId = do
	readS <- readIORef ioReadSet
	validateReadSet2 (Set.toList readS) readStamp myId

--validateReadSet2 :: ReadSet -> Int -> Int-> IO Bool
validateReadSet2 :: [RSEntry] -> Integer -> Integer -> IO Bool
validateReadSet2 [] readStamp myid 		   = return True
validateReadSet2 ((RSE _ lock iowstamp _):ls) readStamp myid = do
				--	locked <- isLocked lock
					lockValue <- readLock lock
					--owner <- readIORef lock
				--	wstamp <- readIORef iowstamp
					if ((isLocked lockValue) && (lockValue /= myid))
						then return False
						else (if (lockValue/=myid) 
							then (if (lockValue > readStamp) then return False else validateReadSet2 ls readStamp myid)
						  	else (do wstamp <- readIORef iowstamp				
								 if (wstamp>readStamp) 
									then return False
									else validateReadSet2 ls readStamp myid))
							

--------------------
--------------
-----
castToPtr:: a -> IO (Ptr ())
castToPtr value = do  stbptr <- newStablePtr value
		      return $ castStablePtrToPtr stbptr

castFromPtr :: Ptr () -> IO a
castFromPtr ptr = deRefStablePtr (castPtrToStablePtr ptr)
------



writeTVar :: TVar a -> a -> STM ()
writeTVar tvar@(TVar lock id writeStamp content queue) newValue = STM $ \tState -> do
        ptr <- castToPtr (WSE lock writeStamp content newValue queue)
	putWS (writeSet tState) id ptr
	return (Valid tState ())

modifyTVar :: TVar a -> (a -> a) -> STM ()
modifyTVar var f = do
    x <- readTVar var
    writeTVar var (f x)

readTVar :: TVar a -> STM a
readTVar tvar@(TVar lock id iorefwstamp content queue) = STM $ \tState -> do
	mptr <- lookUpWS (writeSet tState) id
	case mptr of
		Just ptr -> do 	(WSE _ _ _ v _)<- castFromPtr ptr
			       	return (Valid tState v)
		Nothing -> do
			   lockValue <- readLock lock
                           if (isLocked lockValue) 
				then return (Invalid tState)
				else do
					result <- readIORef content
			        	lockValue2 <- readLock lock
					if ((lockValue /= lockValue2) || (lockValue > (readStamp tState)))
						then return (Invalid tState)
						else do 
							putRS (readSet tState) (RSE id lock iorefwstamp queue)
							return (Valid tState result)


readTVarIO :: TVar a -> IO a
readTVarIO = atomically . readTVar