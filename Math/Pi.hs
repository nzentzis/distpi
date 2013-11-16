{-# LANGUAGE BangPatterns #-}
module Math.Pi (
	bbp
	) where

-- Implementation based on en.literateprograms.org/Pi_with_the_BBP_formula_(Python)

import Data.Bits
import Data.List
import Data.Fixed

-- Modular exponentiation base 16 using the right-to-left binary method
powm :: Int -> Int -> Int -> Int
powm b e m = powm' b e m 1
	where	powm' b 0 m !r = r
		powm' b e m !r = powm' (b^2 `mod` m) (e `shiftR` 1) m $
			if testBit e 0 then (r*b `mod` m) else r

-- Evaluates an 'infinite' sum until it converges
converge :: (Int -> Double) -> Double -> Double
converge f maxErr = converge' f 0 0
	where	converge' f s i = if (abs $ f i) < maxErr then s + f i else
			converge' f (s + f i) (i + 1)

series :: Int -> Int -> Double
series j n = leftSum + rightSum
	where	leftSum = sum $ map
			(\k->(fromIntegral $ powm 16 (n-k) (8*k+j)) /
				(fromIntegral $ 8*k+j))
			[0,1..n]
		rightSum = converge
			(\k->(16**(fromIntegral $ n-(k+n+1))) /
				(fromIntegral $ 8*(k+n+1)+j))
			1e-10

-- Extract the first hex digit of the given value
hexDigit :: Int -> Int
hexDigit !n = if n < 16 then n else hexDigit (n `div` 16)

-- Compute the Nth hex digit of Pi
bbp :: Int -> Int
bbp n = hexDigit $ truncate (v * 16^14)
	where	sn x = series x (n-1)
		v = (4*(sn 1) - 2*(sn 4) - sn 5 - sn 6) `mod'` 1.0
