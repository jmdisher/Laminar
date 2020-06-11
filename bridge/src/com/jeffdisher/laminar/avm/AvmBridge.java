package com.jeffdisher.laminar.avm;

import java.util.List;
import java.util.UUID;

import org.aion.avm.core.AvmConfiguration;
import org.aion.avm.core.AvmImpl;
import org.aion.avm.core.CommonAvmFactory;
import org.aion.avm.core.ExecutionType;
import org.aion.avm.core.FutureResult;
import org.aion.types.Transaction;

import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.utils.Assert;


/**
 * A wrapper over the AVM.
 * Note that the constructor returns in a running state so shutdown() must be explicitly called.
 */
public class AvmBridge {
	private final OneCallCapabilities _capabilities;
	private final AvmImpl _avm;

	public AvmBridge() {
		_capabilities = new OneCallCapabilities();
		AvmConfiguration config = new AvmConfiguration();
		// We only interact with AVM with 1 mutation at a time so running multiple threads is pointless.
		config.threadCount = 1;
		_avm = CommonAvmFactory.buildAvmInstanceForConfiguration(_capabilities, config);
	}

	public List<Consequence> runCreate(TopicContext context, long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce, TopicName topicName, byte[] code, byte[] arguments) {
		Transaction[] transactions = new Transaction[] {
				LaminarAvmTranscoder.createTopic(clientId, clientNonce, topicName, code, arguments)
		};
		ExecutionType executionType = ExecutionType.ASSUME_MAINCHAIN;
		long commonMainchainBlockNumber = globalOffset - 1L;
		_capabilities.setCurrentTopic(topicName);
		LaminarExternalStateAdapter adapter = new LaminarExternalStateAdapter(context, globalOffset, null);
		FutureResult[] results = _avm.run(adapter, transactions, executionType, commonMainchainBlockNumber);
		Assert.assertTrue(transactions.length == results.length);
		return adapter.createOutputConsequences(results[0], termNumber, initialLocalOffset, clientId, clientNonce);
	}

	public List<Consequence> runPut(TopicContext context, long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce, TopicName topicName, byte[] key, byte[] value) {
		Transaction[] transactions = new Transaction[] {
				LaminarAvmTranscoder.invokeTopic(clientId, clientNonce, topicName, key)
		};
		ExecutionType executionType = ExecutionType.ASSUME_MAINCHAIN;
		long commonMainchainBlockNumber = globalOffset - 1L;
		_capabilities.setCurrentTopic(topicName);
		// We distinguish between the PUT and DELETE within the adapter - value is exposed via reading from the key-value store.
		Assert.assertTrue(null != value);
		LaminarExternalStateAdapter adapter = new LaminarExternalStateAdapter(context, globalOffset, value);
		FutureResult[] results = _avm.run(adapter, transactions, executionType, commonMainchainBlockNumber);
		Assert.assertTrue(transactions.length == results.length);
		return adapter.createOutputConsequences(results[0], termNumber, initialLocalOffset, clientId, clientNonce);
	}

	public List<Consequence> runDelete(TopicContext context, long termNumber, long globalOffset, long initialLocalOffset, UUID clientId, long clientNonce, TopicName topicName, byte[] key) {
		Transaction[] transactions = new Transaction[] {
				LaminarAvmTranscoder.invokeTopic(clientId, clientNonce, topicName, key)
		};
		ExecutionType executionType = ExecutionType.ASSUME_MAINCHAIN;
		long commonMainchainBlockNumber = globalOffset - 1L;
		_capabilities.setCurrentTopic(topicName);
		// We distinguish between the PUT and DELETE within the adapter - value is exposed via reading from the key-value store.
		LaminarExternalStateAdapter adapter = new LaminarExternalStateAdapter(context, globalOffset, null);
		FutureResult[] results = _avm.run(adapter, transactions, executionType, commonMainchainBlockNumber);
		Assert.assertTrue(transactions.length == results.length);
		return adapter.createOutputConsequences(results[0], termNumber, initialLocalOffset, clientId, clientNonce);
	}

	public void shutdown() {
		_avm.shutdown();
	}
}
