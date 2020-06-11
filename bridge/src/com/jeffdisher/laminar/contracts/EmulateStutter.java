package com.jeffdisher.laminar.contracts;

import avm.Blockchain;


/**
 * A test contract which emulates the behaviour of the "STUTTER" test message when receiving a normal PUT:  generates
 * two of the same consequences.
 * Handles DELETE messages just as a single DELETE consequence.
 */
public class EmulateStutter {
	public static byte[] main() {
		// We just want to generate the 2 output intentions if this is a PUT.
		byte[] key = Blockchain.getData();
		// (AVM requires that we use a 32-byte key).
		byte[] value = Blockchain.getStorage(new byte[32]);
		if (null != value) {
			// This is a PUT so generate the 2 consequences.
			Blockchain.putStorage(key, value);
			Blockchain.putStorage(key, value);
		} else {
			// This is a DELETE so just generate the normal delete.
			Blockchain.putStorage(key, null);
		}
		return new byte[0];
	}
}
