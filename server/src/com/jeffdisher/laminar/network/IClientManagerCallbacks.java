package com.jeffdisher.laminar.network;

import java.util.UUID;
import java.util.function.Consumer;

import com.jeffdisher.laminar.state.StateSnapshot;
import com.jeffdisher.laminar.types.TopicName;
import com.jeffdisher.laminar.types.message.ClientMessage;
import com.jeffdisher.laminar.types.mutation.MutationRecord;


public interface IClientManagerCallbacks {
	void ioEnqueueClientCommandForMainThread(Consumer<StateSnapshot> command);

	/**
	 * Called to provide a message which arrived from a normal client.
	 * The nonce management and ACKs are all handled on the caller side, based on the response from this method:  either
	 * being received+committed (after delay) or error.
	 * 
	 * @param clientId The UUID of the client which send the message.
	 * @param incoming The message received.
	 * @return 0 if this the message was an error or a positive global mutation offset of the message if it should be
	 * acked normally (the caller will also delay a commit until notified that this mutation offset is durable).
	 */
	long mainHandleValidClientMessage(UUID clientId, ClientMessage incoming);

	/**
	 * Called when the ClientManager wishes to use a mutation in client reconnect and needs it to be loaded.
	 * Note that the receiver can respond to this in 2 different ways:
	 * 1) return immediately if this is in-memory.
	 * 2) schedule that it be fetched from disk.
	 * 
	 * @param mutationOffset The offset to fetch/return/await.
	 * @return The mutation, only if it was immediately available, in-memory (typically not committed).
	 */
	MutationRecord mainClientFetchMutationIfAvailable(long mutationOffset);

	void mainRequestConsequenceFetch(TopicName topic, long nextLocalConsequenceToFetch);

	/**
	 * A testing method to force the node to start an election in response to a testing message.
	 */
	void mainForceLeader();
}
