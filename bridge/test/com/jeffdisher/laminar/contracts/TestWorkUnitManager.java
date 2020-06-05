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
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.event.EventRecord;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


public class TestWorkUnitManager {
	@Test
	public void testDeployment() throws Throwable {
		byte[] code = ContractPackager.createJarForClass(WorkUnitManager.class);
		
		long termNumber = 1L;
		long globalOffset = 1L;
		long initialLocalOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TopicName topic = TopicName.fromString("test");
		AvmBridge bridge = new AvmBridge();
		
		TopicContext context = new TopicContext();
		List<EventRecord> records = bridge.runCreate(context, termNumber, globalOffset, initialLocalOffset, clientId, clientNonce, topic, code, new byte[0]);
		Assert.assertEquals(0, records.size());
		Assert.assertNotNull(context.transformedCode);
		Assert.assertNotNull(context.objectGraph);
		
		bridge.shutdown();
	}

	@Test
	public void testCounting() throws Throwable {
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		
		TestingServer server = new TestingServer(clientId, clientNonce);
		clientNonce += 1;
		
		// Show a failure for 0 or 2 since 1 is expected.
		List<EventRecord> records = server.count(clientId, clientNonce, 0);
		Assert.assertNull(records);
		records = server.count(clientId, clientNonce, 2);
		Assert.assertNull(records);
		
		// If we start with 1, we can send a sequence.
		records = server.count(clientId, clientNonce, 1);
		Assert.assertEquals(1, records.size());
		Assert.assertEquals(1, ByteBuffer.wrap(((Payload_KeyPut)records.get(0).payload).value).getInt());
		
		records = server.count(clientId, clientNonce, 2);
		Assert.assertEquals(1, records.size());
		Assert.assertEquals(2, ByteBuffer.wrap(((Payload_KeyPut)records.get(0).payload).value).getInt());
		
		server.stop();
	}


	private static class TestingServer {
		private static final long TERM_NUMBER = 1L;
		
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
			
			byte[] code = ContractPackager.createJarForClass(WorkUnitManager.class);
			List<EventRecord> records = _bridge.runCreate(_context, TERM_NUMBER, _nextGlobalOffset, _nextLocalOffset, clientId, clientNonce, _topic, code, new byte[0]);
			Assert.assertEquals(0, records.size());
			Assert.assertNotNull(_context.transformedCode);
			Assert.assertNotNull(_context.objectGraph);
			_nextGlobalOffset += 1;
			_nextLocalOffset += records.size();
		}
		
		public List<EventRecord> count(UUID clientId, long clientNonce, int numberToRequest) {
			List<EventRecord> records = _bridge.runPut(_context, TERM_NUMBER, _nextGlobalOffset, _nextLocalOffset, clientId, clientNonce, _topic, new byte[32], ByteBuffer.allocate(Integer.BYTES).putInt(numberToRequest).array());
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
