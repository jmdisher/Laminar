package com.jeffdisher.laminar.types;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;


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
		byte[] payload = new byte[] { 1, 2, 3 };
		EventRecord record = EventRecord.generateRecord(EventRecordType.TEMP, termNumber, globalOffset, localOffset, clientId, clientNonce, payload);
		byte[] serialized = record.serialize();
		EventRecord deserialized = EventRecord.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.localOffset, deserialized.localOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(record.payload, deserialized.payload);
	}
}
