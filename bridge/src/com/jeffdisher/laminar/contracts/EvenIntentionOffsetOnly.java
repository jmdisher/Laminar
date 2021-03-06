package com.jeffdisher.laminar.contracts;

import avm.Blockchain;


/**
 * This contract will pass all PUT and DELETE calls through to consequences but only when the intention offset is even.
 * Otherwise, it will fail.
 */
public class EvenIntentionOffsetOnly {
	private static long _previousIntentionOffset;

	public static byte[] main() {
		// Just verify that our invariant is true.
		Blockchain.require(0L == (_previousIntentionOffset % 2L));
		// We capture this here to make sure that the graph doesn't write-back, on failure.
		_previousIntentionOffset = Blockchain.getBlockNumber();
		// Similarly, create an consequence which should be dropped on the failure.
		if (0L != (Blockchain.getBlockNumber() % 2L)) {
			// (this should NOT be observable under any circumstances).
			Blockchain.putStorage(new byte[32], new byte[] {42});
		}
		// Post the consequence (this will work on PUT and DELETE but will be reverted if the final require fails).
		byte[] key = Blockchain.getData();
		byte[] value = Blockchain.getStorage(key);
		Blockchain.putStorage(key, value);
		
		// We will fail and revert everything if the intention offset isn't an even number.
		Blockchain.require(0L == (Blockchain.getBlockNumber() % 2L));
		return new byte[0];
	}
}
