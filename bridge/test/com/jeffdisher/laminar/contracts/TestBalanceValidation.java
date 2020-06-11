package com.jeffdisher.laminar.contracts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.avm.AvmBridge;
import com.jeffdisher.laminar.avm.ContractPackager;
import com.jeffdisher.laminar.avm.TopicContext;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


public class TestBalanceValidation {
	@Test
	public void testDeployment() throws Throwable {
		byte[] code = ContractPackager.createJarForClass(AccountBalanceValidation.class);
		byte[] arguments = ByteBuffer.allocate(Short.BYTES).putShort((short)2).array();
		
		long termNumber = 1L;
		long globalOffset = 1L;
		long initialLocalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TopicName topic = TopicName.fromString("test");
		AvmBridge bridge = new AvmBridge();
		
		TopicContext context = new TopicContext();
		List<Consequence> records = bridge.runCreate(context, termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, code, arguments);
		Assert.assertEquals(0, records.size());
		Assert.assertNotNull(context.transformedCode);
		Assert.assertNotNull(context.objectGraph);
		
		bridge.shutdown();
	}

	@Test
	public void testSimplePuts() throws Throwable {
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TestingServer server = new TestingServer(clientId, clientNonce);
		clientNonce += 1;
		
		// Define accounts.
		byte[] bank = _createAccountKey(0);
		byte[] account1 = _createAccountKey(1);
		byte[] account2 = _createAccountKey(2);
		
		// Create the mint for account1.
		int account1Balance = 100;
		byte[] mintAccount1 = _packagePut(account1, 0L, account1Balance, 0, 0);
		List<Consequence> records = server.put(clientId, clientNonce, bank, mintAccount1);
		// We don't see the bank send the money - just the account receive it and the transfer consequence.
		Assert.assertEquals(2, records.size());
		clientNonce += 1;
		
		// Transfer to account2.
		int account2Balance = 50;
		byte[] transferTo2 = _packagePut(account2, records.get(0).intentionOffset, account2Balance, account1Balance, 0);
		records = server.put(clientId, clientNonce, account1, transferTo2);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		clientNonce += 1;
		
		server.stop();
	}

	@Test
	public void testStaleChange() throws Throwable {
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TestingServer server = new TestingServer(clientId, clientNonce);
		clientNonce += 1;
		
		// Define accounts.
		byte[] bank = _createAccountKey(0);
		byte[] account1 = _createAccountKey(1);
		byte[] account2 = _createAccountKey(2);
		
		// Create the mint for account1.
		int account1Balance = 100;
		byte[] mintAccount1 = _packagePut(account1, 0L, account1Balance, 0, 0);
		List<Consequence> records = server.put(clientId, clientNonce, bank, mintAccount1);
		// We don't see the bank send the money - just the account receive it and the transfer consequence.
		Assert.assertEquals(2, records.size());
		clientNonce += 1;
		
		// Transfer to account2.
		int account2Balance = 90;
		byte[] transferTo2 = _packagePut(account2, records.get(0).intentionOffset, account2Balance, account1Balance, 0);
		records = server.put(clientId, clientNonce, account1, transferTo2);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		clientNonce += 1;
		
		// Duplicate that account transfer call but it should fail since the account was updated since then.
		records = server.put(clientId, clientNonce, account1, transferTo2);
		Assert.assertNull(records);
		
		server.stop();
	}

	/**
	 * DELETE messages are not supported by this program so they should fail.
	 */
	@Test
	public void testDeleteFailure() throws Throwable {
		byte[] code = ContractPackager.createJarForClass(AccountBalanceValidation.class);
		byte[] arguments = ByteBuffer.allocate(Short.BYTES).putShort((short)2).array();
		
		long termNumber = 1L;
		long globalOffset = 1L;
		long initialLocalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TopicName topic = TopicName.fromString("test");
		AvmBridge bridge = new AvmBridge();
		
		TopicContext context = new TopicContext();
		List<Consequence> records = bridge.runCreate(context, termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, code, arguments);
		Assert.assertEquals(0, records.size());
		Assert.assertNotNull(context.transformedCode);
		Assert.assertNotNull(context.objectGraph);
		globalOffset += 1L;
		
		// Deletes are not support so this should return null.
		records = bridge.runDelete(context, termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, new byte[32]);
		Assert.assertNull(records);
		
		bridge.shutdown();
	}

	/**
	 * A common sequence of transfer operations which would succeed.
	 */
	@Test
	public void testCommonSuccessSequence() throws Throwable {
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TestingServer server = new TestingServer(clientId, clientNonce);
		clientNonce += 1;
		
		// Define accounts.
		byte[] bank = _createAccountKey(0);
		byte[] account1 = _createAccountKey(1);
		byte[] account2 = _createAccountKey(2);
		byte[] account3 = _createAccountKey(3);
		byte[] account4 = _createAccountKey(4);
		
		// Create the mint for account1.
		int account1Balance = 1000;
		byte[] mintAccount1 = _packagePut(account1, 0L, account1Balance, 0, 0);
		List<Consequence> records = server.put(clientId, clientNonce, bank, mintAccount1);
		// We don't see the bank send the money - just the account receive it and the transfer consequence.
		Assert.assertEquals(2, records.size());
		_checkBalance(records.get(0), account1, 1000);
		_checkTransfer(records.get(1), 1000);
		clientNonce += 1;
		long mutationOffsetOfFirstTransfer = records.get(0).intentionOffset;
		// Cache contents: 1
		
		// Transfer 1 to account2.
		int account2Balance = 500;
		byte[] transfer1To2 = _packagePut(account2, mutationOffsetOfFirstTransfer, account2Balance, account1Balance, 0);
		records = server.put(clientId, clientNonce, account1, transfer1To2);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account1, 500);
		_checkBalance(records.get(1), account2, 500);
		_checkTransfer(records.get(2), 500);
		clientNonce += 1;
		long mutationOffset_transfer1To2 = records.get(0).intentionOffset;
		// Cache contents: 1,2
		
		// Transfer 1 to account3 - done as thought it didn't see transfer1To2.
		int account3Balance = 300;
		byte[] transfer1To3 = _packagePut(account3, mutationOffsetOfFirstTransfer, account3Balance, account1Balance, 0);
		records = server.put(clientId, clientNonce, account1, transfer1To3);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account1, 200);
		_checkBalance(records.get(1), account3, 300);
		_checkTransfer(records.get(2), 300);
		clientNonce += 1;
		long mutationOffset_transfer1To3 = records.get(0).intentionOffset;
		// Cache contents: 3,1,2
		
		// Transfer 2 to account4 - done as thought it didn't see transfer1To3.
		int account4Balance = 400;
		byte[] transfer2To4 = _packagePut(account4, mutationOffset_transfer1To2, account4Balance, account2Balance, 0);
		records = server.put(clientId, clientNonce, account2, transfer2To4);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account2, 100);
		_checkBalance(records.get(1), account4, 400);
		_checkTransfer(records.get(2), 400);
		clientNonce += 1;
		long mutationOffset_transfer2To4 = records.get(0).intentionOffset;
		// Cache contents: 4,2,3
		
		// Transfer 1 to account4 - done as thought it didn't see transfer2To4.
		int account4Transfer = 200;
		byte[] transfer1To4 = _packagePut(account4, mutationOffset_transfer1To3, account4Transfer, account1Balance - account2Balance - account3Balance, 0);
		records = server.put(clientId, clientNonce, account1, transfer1To4);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account1, 0);
		_checkBalance(records.get(1), account4, 600);
		_checkTransfer(records.get(2), 200);
		clientNonce += 1;
		// Cache contents: 4,1,2
		
		// Transfer 4 to account3 - 3 shouldn't be in cache but lets assume we only saw mutationOffset_transfer2To4 (we will send an incorrect source balance - since we didn't see transfer1To4 - but it is in cache so it will be corrected and pass since only the destination is missing).
		int account3Transfer = 400;
		byte[] transfer4To3 = _packagePut(account3, mutationOffset_transfer2To4, account3Transfer, 400, 300);
		records = server.put(clientId, clientNonce, account4, transfer4To3);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account4, 200);
		_checkBalance(records.get(1), account3, 700);
		_checkTransfer(records.get(2), 400);
		clientNonce += 1;
		// Cache contents: 3,4,1
		
		server.stop();
	}

	/**
	 * A common sequence of transfer operations which would fail due to a detected race.
	 */
	@Test
	public void testCommonFailureSequence() throws Throwable {
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TestingServer server = new TestingServer(clientId, clientNonce);
		clientNonce += 1;
		
		// Define accounts.
		byte[] bank = _createAccountKey(0);
		byte[] account1 = _createAccountKey(1);
		byte[] account2 = _createAccountKey(2);
		byte[] account3 = _createAccountKey(3);
		byte[] account4 = _createAccountKey(4);
		
		// Create the mint for account1.
		int account1Balance = 1000;
		byte[] mintAccount1 = _packagePut(account1, 0L, account1Balance, 0, 0);
		List<Consequence> records = server.put(clientId, clientNonce, bank, mintAccount1);
		// We don't see the bank send the money - just the account receive it and the transfer consequence.
		Assert.assertEquals(2, records.size());
		_checkBalance(records.get(0), account1, 1000);
		_checkTransfer(records.get(1), 1000);
		clientNonce += 1;
		long mutationOffsetOfFirstTransfer = records.get(0).intentionOffset;
		
		// Transfer 1 to account2.
		int account2Balance = 500;
		byte[] transfer1To2 = _packagePut(account2, mutationOffsetOfFirstTransfer, account2Balance, account1Balance, 0);
		records = server.put(clientId, clientNonce, account1, transfer1To2);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account1, 500);
		_checkBalance(records.get(1), account2, 500);
		_checkTransfer(records.get(2), 500);
		clientNonce += 1;
		
		// Transfer 1 to account3 - done as thought it didn't see transfer1To2.
		int account3Balance = 300;
		byte[] transfer1To3 = _packagePut(account3, mutationOffsetOfFirstTransfer, account3Balance, account1Balance, 0);
		records = server.put(clientId, clientNonce, account1, transfer1To3);
		// We see the send, receive, and transfer.
		Assert.assertEquals(3, records.size());
		_checkBalance(records.get(0), account1, 200);
		_checkBalance(records.get(1), account3, 300);
		_checkTransfer(records.get(2), 300);
		clientNonce += 1;
		
		// By this point, the cache is full:  3,1,2.
		// We can start interacting with 4 in ways which will fail or evict from the cache.
		
		// Transfer 2 to account4 - done as thought it didn't see anything - this will fail since the cache is full so it might be missing data.
		int account4Balance = 400;
		byte[] transfer2To4 = _packagePut(account4, 0L, account4Balance, account2Balance, 0);
		records = server.put(clientId, clientNonce, account2, transfer2To4);
		Assert.assertNull(records);
		clientNonce += 1;
		
		server.stop();
	}


	private byte[] _packagePut(byte[] destinationKey, long clientLastSeenMutationOffset, int value, int lastSenderValue, int lastDestinationValue) {
		return ByteBuffer.allocate(destinationKey.length + Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES)
				.put(destinationKey)
				.putLong(clientLastSeenMutationOffset)
				.putInt(value)
				.putInt(lastSenderValue)
				.putInt(lastDestinationValue)
				.array();
	}

	private byte[] _createAccountKey(int value) {
		// Due to blockchain-specific validations in the AVM API, keys need to be 32 bytes.
		byte[] key = new byte[32];
		key[0] = (byte)value;
		return key;
	}

	private void _checkBalance(Consequence eventRecord, byte[] account, int balance) {
		Assert.assertEquals(Consequence.Type.KEY_PUT, eventRecord.type);
		Assert.assertArrayEquals(account, ((Payload_KeyPut)eventRecord.payload).key);
		Assert.assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(balance).array(), ((Payload_KeyPut)eventRecord.payload).value);
	}

	private void _checkTransfer(Consequence eventRecord, int balance) {
		Assert.assertEquals(Consequence.Type.KEY_PUT, eventRecord.type);
		Assert.assertArrayEquals(new byte[32], ((Payload_KeyPut)eventRecord.payload).key);
		Assert.assertArrayEquals(ByteBuffer.allocate(Integer.BYTES).putInt(balance).array(), ((Payload_KeyPut)eventRecord.payload).value);
	}


	private static class TestingServer {
		private static final long TERM_NUMBER = 1L;
		// We will use a 3-element cache so tests of eviction will need at least 4 accounts.
		private static final short CACHE_SIZE = 3;
		
		private final TopicName _topic;
		private final AvmBridge _bridge;
		private final TopicContext _context;
		private long _nextGlobalOffset;
		private long _nextLocalOffset;
		
		public TestingServer(UUID clientId, long clientNonce) throws IOException {
			_topic = TopicName.fromString("test");
			_bridge = new AvmBridge();
			_context = new TopicContext();
			
			_nextGlobalOffset = 1;
			_nextLocalOffset = 1;
			
			byte[] code = ContractPackager.createJarForClass(AccountBalanceValidation.class);
			byte[] arguments = ByteBuffer.allocate(Short.BYTES).putShort(CACHE_SIZE).array();
			List<Consequence> records = _bridge.runCreate(_context, TERM_NUMBER, _nextGlobalOffset, _nextLocalOffset, clientId, clientNonce, _topic, code, arguments);
			Assert.assertEquals(0, records.size());
			Assert.assertNotNull(_context.transformedCode);
			Assert.assertNotNull(_context.objectGraph);
			_nextGlobalOffset += 1;
			_nextLocalOffset += records.size();
		}
		
		public List<Consequence> put(UUID clientId, long clientNonce, byte[] senderKey, byte[] payload) {
			List<Consequence> records = _bridge.runPut(_context, TERM_NUMBER, _nextGlobalOffset, _nextLocalOffset, clientId, clientNonce, _topic, senderKey, payload);
			_nextGlobalOffset += 1;
			_nextLocalOffset += (null != records)
					? records.size()
					: 0L;
			return records;
		}
		
		public void stop() {
			_bridge.shutdown();
		}
	}
}
