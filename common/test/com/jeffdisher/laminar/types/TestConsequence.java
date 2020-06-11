package com.jeffdisher.laminar.types;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.payload.Payload_KeyPut;


/**
 * Tests around serialization and deserialization of Consequence objects.
 */
public class TestConsequence {
	@Test
	public void testBasic() throws Throwable {
		long termNumber = 1L;
		long globalOffset = 1L;
		long localOffset = 1L;
		UUID clientId = UUID.randomUUID();
		long clientNonce = 1L;
		byte[] key = "key".getBytes(StandardCharsets.UTF_8);
		byte[] value = "value".getBytes(StandardCharsets.UTF_8);
		Consequence record = Consequence.put(termNumber, globalOffset, localOffset, clientId, clientNonce, key, value);
		byte[] serialized = record.serialize();
		Consequence deserialized = Consequence.deserialize(serialized);
		Assert.assertEquals(record.type, deserialized.type);
		Assert.assertEquals(record.termNumber, deserialized.termNumber);
		Assert.assertEquals(record.globalOffset, deserialized.globalOffset);
		Assert.assertEquals(record.consequenceOffset, deserialized.consequenceOffset);
		Assert.assertEquals(record.clientId, deserialized.clientId);
		Assert.assertEquals(record.clientNonce, deserialized.clientNonce);
		Assert.assertArrayEquals(((Payload_KeyPut)record.payload).key, ((Payload_KeyPut)deserialized.payload).key);
		Assert.assertArrayEquals(((Payload_KeyPut)record.payload).value, ((Payload_KeyPut)deserialized.payload).value);
		Assert.assertArrayEquals(key, ((Payload_KeyPut)deserialized.payload).key);
		Assert.assertArrayEquals(value, ((Payload_KeyPut)deserialized.payload).value);
	}
}
