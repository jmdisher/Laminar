package com.jeffdisher.laminar.types.event;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.payload.Payload_Put;


/**
 * Tests around serialization and deserialization of EventRecord objects.
 */
public class TestEventRecord {
	@Test
	public void testBasic() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		long localOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = "key".getBytes(StandardCharsets.UTF_8);
		byte[] value = "value".getBytes(StandardCharsets.UTF_8);
		EventRecord record = EventRecord.put(termNumber, globalOffset, localOffset, clientId, clientNonce, key, value);
		byte[] serialized = record.serialize();
		EventRecord deserialized = EventRecord.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.localOffset, deserialized.localOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(((Payload_Put)record.payload).key, ((Payload_Put)deserialized.payload).key);
		Assert.assertArrayEquals(((Payload_Put)record.payload).value, ((Payload_Put)deserialized.payload).value);
		Assert.assertArrayEquals(key, ((Payload_Put)deserialized.payload).key);
		Assert.assertArrayEquals(value, ((Payload_Put)deserialized.payload).value);
	}
}
