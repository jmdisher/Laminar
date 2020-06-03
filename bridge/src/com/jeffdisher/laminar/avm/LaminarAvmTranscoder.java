package com.jeffdisher.laminar.avm;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.aion.avm.userlib.CodeAndArguments;
import org.aion.types.AionAddress;
import org.aion.types.Transaction;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


public class LaminarAvmTranscoder {
	private static BigInteger VALUE = BigInteger.ZERO;
	private static long ENERGY_LIMIT = 1_000_000L;
	private static long ENERGY_PRICE = 1L;

	public static Transaction createTopic(UUID clientId, long clientNonce, TopicName topicName, byte[] code, byte[] arguments) {
		AionAddress sender = _addressFromUuid(clientId);
		byte[] transactionHash = new byte[0];
		BigInteger senderNonce = BigInteger.valueOf(clientNonce);
		byte[] codeAndArgs = new CodeAndArguments(code, arguments).encodeToBytes();
		return Transaction.contractCreateTransaction(sender, transactionHash, senderNonce, VALUE, codeAndArgs, ENERGY_LIMIT, ENERGY_PRICE);
	}

	public static Transaction invokeTopic(UUID clientId, long clientNonce, TopicName topicName, byte[] key) {
		AionAddress sender = _addressFromUuid(clientId);
		AionAddress destination = _addressFromTopicName(topicName);
		byte[] transactionHash = new byte[0];
		BigInteger senderNonce = BigInteger.valueOf(clientNonce);
		return Transaction.contractCallTransaction(sender, destination, transactionHash, senderNonce, VALUE, key, ENERGY_LIMIT, ENERGY_PRICE);
	}

	public static AionAddress addressFromTopicName(TopicName topicName) {
		return _addressFromTopicName(topicName);
	}

	private static AionAddress _addressFromUuid(UUID clientId) {
		long most = clientId.getMostSignificantBits();
		long least = clientId.getLeastSignificantBits();
		ByteBuffer buffer = ByteBuffer.allocate(AionAddress.LENGTH);
		buffer.position(AionAddress.LENGTH - Long.BYTES - Long.BYTES);
		byte[] raw = buffer
				.putLong(most)
				.putLong(least)
				.array();
		return new AionAddress(raw);
	}

	private static AionAddress _addressFromTopicName(TopicName topicName) {
		// We currently use the TopicName as the address so make sure that it fits.
		Assert.assertTrue(topicName.serializedSize() <= AionAddress.LENGTH);
		ByteBuffer addressContainer = ByteBuffer.allocate(AionAddress.LENGTH);
		topicName.serializeInto(addressContainer);
		return new AionAddress(addressContainer.array());
	}
}
