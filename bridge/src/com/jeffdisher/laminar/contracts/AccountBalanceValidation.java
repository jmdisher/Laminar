package com.jeffdisher.laminar.contracts;

import java.util.Arrays;

import avm.Blockchain;


/**
 * The contract which implements account balance transfer validation (an example problem Laminar was designed to solve).
 * The implementation is very simple, but wouldn't scale to a large cache.  For the purposes of the test, however, it
 * will be sufficient.
 * 
 * Deployment argument:
 * -2-byte big-endian Short representing the size of the cache to keep.
 * 
 * PUT value argument:
 * -32-byte destination key (all zero if this is burning)
 * -8-byte big-endian long of mutation offset corresponding to last consequence seen by client
 * -4-byte big-endian int of value to withdraw
 * -4-byte big-endian int of source value at last mutation offset seen by client
 * -4-byte big-endian int of destination value at last mutation offset seen by client
 * 
 * The key in the PUT is the source of the transfer (all zero if this is minting).
 * 
 * In order to allow listeners to understand either the final result of the account states or the fact that a
 * transaction was processed (and, more importantly, to provide a "terminator" for the sequence of consequences in one
 * mutation), we will generate up to 3 consequences per PUT:
 * -key(source) -> balance(source) - (not present if source was minting)
 * -key(destination) ->  balance(destination) - (not present if destination was burning)
 * -key("transaction") -> value
 */
public class AccountBalanceValidation {
	private static final CacheEntry[] CACHE;
	private static final byte[] BANK;
	private static final byte[] TRANSACTION_KEY;

	static {
		short cacheSize = Coder.wrap(Blockchain.getData()).getShort();
		CACHE = new CacheEntry[(int)cacheSize];
		BANK = new byte[32];
		TRANSACTION_KEY = new byte[32];
	}

	public static byte[] main() {
		byte[] key = Blockchain.getData();
		// We just need any 32-byte value for the storage so use the key.
		byte[] value = Blockchain.getStorage(key);
		// Deletes aren't interpreted in this test.
		Blockchain.require(null != value);
		Coder wrapper = Coder.wrap(value);
		byte[] destination = new byte[32];
		wrapper.get(destination);
		long knownMutationOffset = wrapper.getLong();
		int valueToMove = wrapper.getInt();
		int knownSourceBalance = wrapper.getInt();
		int knownDestinationBalance = wrapper.getInt();
		
		// Look-up the oldest entry offset before we start changing the cache.
		long oldestCacheOffset = _getOldestOffsetInCache();
		
		CacheEntry sourceEntry = null;
		if (Arrays.equals(BANK, key)) {
			// No check required - we can just proceed.
		} else {
			// See if this is present in the cache.
			sourceEntry = _checkoutOrSynthesizeEntry(key, knownMutationOffset, knownSourceBalance, oldestCacheOffset);
			// Make sure the transaction is valid.
			Blockchain.require(sourceEntry.balance >= valueToMove);
			
			// Perform the update and generate the consequence.
			sourceEntry.balance -= valueToMove;
			Blockchain.putStorage(key, Coder.allocate(Integer.BYTES).putInt(sourceEntry.balance).array());
		}
		
		CacheEntry destinationEntry = null;
		if (Arrays.equals(BANK, destination)) {
			// No destination update on burning.
		} else {
			// We still need to fail if we can't build a correct picture of the destination so we perform the same operation.
			destinationEntry = _checkoutOrSynthesizeEntry(destination, knownMutationOffset, knownDestinationBalance, oldestCacheOffset);
			
			// Perform the update and generate the consequence.
			destinationEntry.balance += valueToMove;
			Blockchain.putStorage(destination, Coder.allocate(Integer.BYTES).putInt(destinationEntry.balance).array());
		}
		
		// Re-populate the cache (done at the end to avoid evictions we might need).
		if (null != sourceEntry) {
			_checkinCache(sourceEntry);
		}
		if (null != destinationEntry) {
			_checkinCache(destinationEntry);
		}
		
		// If we got this far, the transfer is a success so just generate the terminator transfer consequence.
		Blockchain.putStorage(TRANSACTION_KEY, Coder.allocate(Integer.BYTES).putInt(valueToMove).array());
		return new byte[0];
	}


	private static CacheEntry _checkoutOrSynthesizeEntry(byte[] key, long knownMutationOffset, int knownBalance, long oldestCacheOffset) {
		CacheEntry sourceEntry = _checkoutCache(key);
		if (null == sourceEntry) {
			// There is no entry in cache so make sure that the known source is at least as up-to-date as the oldest cache entry.
			// If not, the balance might have changed further back than our cache can see.
			Blockchain.require(knownMutationOffset >= oldestCacheOffset);
			// Synthesize the entry based on our known data since we know it is valid (otherwise, we would have it in cache).
			sourceEntry = new CacheEntry(key, knownBalance);
		}
		return sourceEntry;
	}

	private static long _getOldestOffsetInCache() {
		long oldestMutationOffset = 0;
		for (int i = 0; i < CACHE.length; ++i) {
			if (null != CACHE[i]) {
				oldestMutationOffset = CACHE[i].validOffset;
			} else {
				// The cache isn't full which means we have all the data.  Anything not here, doesn't exist, so we can create it.
				oldestMutationOffset = 0;
				break;
			}
		}
		return oldestMutationOffset;
	}

	private static CacheEntry _checkoutCache(byte[] key) {
		CacheEntry entry = null;
		for (int i = 0; (i < CACHE.length) && (null != CACHE[i]); ++i) {
			if (Arrays.equals(CACHE[i].key, key)) {
				entry = CACHE[i];
				// Shift the rest of the array over.
				int elementsToShift = CACHE.length - i - 1;
				if (elementsToShift > 0) {
					System.arraycopy(CACHE, i + 1, CACHE, i, elementsToShift);
				}
				// The last element will be duplicated so null it (already null if not full).
				CACHE[CACHE.length - 1] = null;
			}
		}
		return entry;
	}

	private static void _checkinCache(CacheEntry entry) {
		// We just modified this so we know it changed in this mutation offset.
		long newOffset = Blockchain.getBlockNumber();
		entry.validOffset = newOffset;
		System.arraycopy(CACHE, 0, CACHE, 1, CACHE.length - 1);
		CACHE[0] = entry;
	}


	private static class CacheEntry {
		public final byte[] key;
		public int balance;
		public long validOffset;
		public CacheEntry(byte[] key, int initialBalance) {
			this.key = key;
			this.balance = initialBalance;
		}
	}


	private static class Coder {
		public static Coder wrap(byte[] data) {
			return new Coder(data);
		}
		
		public static Coder allocate(int bytes) {
			return new Coder(new byte[bytes]);
		}
		
		private final byte[] _buffer;
		private int _cursor;
		
		private Coder(byte[] data) {
			_buffer = data;
			_cursor = 0;
		}
		
		public short getShort() {
			int base = _cursor;
			_cursor += Short.BYTES;
			return (short) ((0xff00 & (_buffer[base] << 8))
					| (0x00ff & _buffer[base + 1])
			);
		}
		
		public int getInt() {
			int base = _cursor;
			_cursor += Integer.BYTES;
			return (  (0xff000000 & (_buffer[base] << 24))
					| (0x00ff0000 & (_buffer[base + 1] << 16))
					| (0x0000ff00 & (_buffer[base + 2] << 8))
					| (0x000000ff & _buffer[base + 3])
			);
		}
		
		public long getLong() {
			int base = _cursor;
			_cursor += Long.BYTES;
			return (  (0xff00000000000000L & (long)(_buffer[base] << 56))
					| (0x00ff000000000000L & (long)(_buffer[base + 1] << 48))
					| (0x0000ff0000000000L & (long)(_buffer[base + 2] << 40))
					| (0x000000ff00000000L & (long)(_buffer[base + 3] << 32))
					| (0x00000000ff000000L & (long)(_buffer[base + 4] << 24))
					| (0x0000000000ff0000L & (long)(_buffer[base + 5] << 16))
					| (0x000000000000ff00L & (long)(_buffer[base + 6] << 8))
					| (0x00000000000000ffL & (long)_buffer[base + 7])
			);
		}
		
		public void get(byte[] destination) {
			int base = _cursor;
			_cursor += destination.length;
			System.arraycopy(_buffer, base, destination, 0, destination.length);
		}
		
		public Coder putInt(int balance) {
			int base = _cursor;
			_cursor += Integer.BYTES;
			_buffer[base] = (byte)(0xff & (balance >> 24));
			_buffer[base+1] = (byte)(0xff & (balance >> 16));
			_buffer[base+2] = (byte)(0xff & (balance >> 8));
			_buffer[base+3] = (byte)(0xff & (balance));
			return this;
		}
		
		public byte[] array() {
			return _buffer;
		}
	}
}
