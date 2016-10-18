{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
module Control.PooledMapConcurrently
  ( pooledMapConcurrently
  , pooledMapConcurrently_
  ) where

import           Control.Concurrent.MVar
import           Control.Monad.Base
import           Control.Monad.Trans.Control
import           Data.IORef
import           Data.Foldable
import           Data.Traversable
import           Control.Concurrent
import           Control.Concurrent.Async.Lifted.Safe (runConcurrently, Concurrently(..), Pure, Forall)

-- | Call 'pooledMapConcurrently' with the number of worker threads set as the
-- number of capabilities.
pooledMapConcurrently_ :: (Traversable t, Forall (Pure m), MonadBaseControl IO m) => (a -> m b) -> t a -> m (t b)
pooledMapConcurrently_ f xs = do
  numProcs <- liftBase getNumCapabilities
  pooledMapConcurrently numProcs f xs

-- | Execute the mapping with a specified number of worker threads.
pooledMapConcurrently :: forall t m a b. (Traversable t, Forall (Pure m), MonadBaseControl IO m) => Int -> (a -> m b) -> t a -> m (t b)
pooledMapConcurrently numThreads f xs = if numThreads < 1
 then error ("Control.PooledMapConcurrently.pooledMapConcurrently: numThreads < 1 (" ++ show numThreads ++ ")")
 else do
   jobs :: t (a, IORef b) <- liftBase (for xs (\x -> (x, ) <$> newIORef (error "Control.Concurrent.pooledMapConcurrently: empty IORef")))
   jobsVar :: MVar [(a, IORef b)] <- liftBase $ newMVar (toList jobs)
   runConcurrently $ for_ [1..numThreads] $ \_ -> Concurrently $ do
     let loop :: m ()
         loop = do
           m'job :: Maybe (a, IORef b) <- liftBase $ modifyMVar jobsVar $ \case
             [] -> return ([], Nothing)
             var : vars -> return (vars, Just var)
           for_ m'job $ \(x, outRef) -> do
             y <- f x
             liftBase (writeIORef outRef y)
             loop
     loop
   liftBase (for jobs (\(_, outputRef) -> readIORef outputRef))
