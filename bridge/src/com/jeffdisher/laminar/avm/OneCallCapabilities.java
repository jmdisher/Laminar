package com.jeffdisher.laminar.avm;

import java.math.BigInteger;

import org.aion.avm.core.IExternalCapabilities;
import org.aion.types.AionAddress;
import org.aion.types.InternalTransaction;

import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Laminar is not a blockchain so none of the hashing or meta-transaction support is implemented.
 * The only thing we support is the creation of new "contract" addresses but we expose those as just a serialized
 * version of the topic name.
 * Currently, we need to set the topic name in this, explicitly, before running a create, since we have no other way to
 * plumb the data in.
 * TODO:  Update this once we change this interface in the AVM to allow extra context to be passed around since this
 * approach requires more knowledge in the caller and means multiple topics can't be created concurrently.
 */
public class OneCallCapabilities implements IExternalCapabilities {
	private TopicName _currentTopic;

	public void setCurrentTopic(TopicName topicName) {
		_currentTopic = topicName;
	}

	@Override
	public boolean verifyEdDSA(byte[] arg0, byte[] arg1, byte[] arg2) {
		throw Assert.unreachable("Crypto not supported");
	}
	@Override
	public byte[] sha256(byte[] arg0) {
		throw Assert.unreachable("Crypto not supported");
	}
	@Override
	public byte[] keccak256(byte[] arg0) {
		throw Assert.unreachable("Crypto not supported");
	}
	@Override
	public AionAddress generateContractAddress(AionAddress deployerAddress, BigInteger nonce) {
		return LaminarAvmTranscoder.addressFromTopicName(_currentTopic);
	}
	@Override
	public InternalTransaction decodeSerializedTransaction(byte[] transactionPayload, AionAddress executor, long energyPrice, long energyLimit) {
		throw Assert.unreachable("Meta-transactions not supported");
	}
	@Override
	public byte[] blake2b(byte[] arg0) {
		throw Assert.unreachable("Crypto not supported");
	}
}
