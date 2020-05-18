package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * This is meant as the read-only shadow of DownstreamPeerState so that all state transitions of the underlying elements
 * can be managed in DownstreamPeerManager, while other components can at least have visibility into the peers.
 * This is because other components should be able to make their own decisions around what to do with a peer but none of
 * them should be able to cause a state change of the peer, inline.
 * Note that this wrapper does allow for a single mutative operation to be performed but this is only granted in a
 * high-level way, invalidates the receiver for future calls, and must only be done when the caller is certain they will
 * perform the change of state.
 */
public class ReadOnlyDownstreamPeerState {
	private final DownstreamPeerState _original;
	private boolean _isValid;

	public final ConfigEntry entry;
	public final NetworkManager.NodeToken token;

	public ReadOnlyDownstreamPeerState(DownstreamPeerState original) {
		_original = original;
		_isValid = true;
		this.entry = original.entry;
		this.token = original.token;
	}

	/**
	 * @return True if the connection is up, writable, and completed a handshake.
	 */
	public boolean isReadyForSend() {
		Assert.assertTrue(_isValid);
		return _original.isConnectionUp
				&& _original.didHandshake
				&& _original.isWritable
		;
	}

	/**
	 * @return True if the peer has a mutation it could send.
	 */
	public boolean hasMutationToSend() {
		Assert.assertTrue(_isValid);
		return (DownstreamPeerState.NO_NEXT_MUTATION != _original.nextMutationOffsetToSend);
	}

	/**
	 * @return True if the peer has a vote is could send.
	 */
	public boolean hasVoteToSend() {
		Assert.assertTrue(_isValid);
		return (null != _original.pendingVoteRequest);
	}

	/**
	 * @return The next mutation offset the receiver could send.
	 */
	public long getNextMutationOffsetToSend() {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(DownstreamPeerState.NO_NEXT_MUTATION != _original.nextMutationOffsetToSend);
		return _original.nextMutationOffsetToSend;
	}

	/**
	 * Called when the caller has decided to send an identity message to the peer.
	 * Mutative operation which invalidates the receiver.
	 * 
	 * @param self The ConfigEntry to send in identity.
	 * @param nowMillis The time of the send.
	 * @return The message which now MUST be sent to the peer.
	 */
	public DownstreamMessage commitToSendIdentity(ConfigEntry self, long nowMillis) {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(_original.isConnectionUp);
		Assert.assertTrue(_original.isWritable);
		// (we don't check the handshake since identity is sent as part of that)
		
		DownstreamMessage message = DownstreamMessage.identity(self);
		_original.isWritable = false;
		_original.lastSentMessageMillis = nowMillis;
		_isValid = false;
		return message;
	}

	/**
	 * Called when the caller has decided to send a mutation to the peer.
	 * Mutative operation which invalidates the receiver.
	 * Note that this assumes the receiver was already ready to send this mutation.
	 * 
	 * @param currentTermNumber The current term of this node.
	 * @param previousMutationTermNumber The term number of the mutation before this one.
	 * @param mutation The mutation to send.
	 * @param lastCommittedMutationOffset The last mutation this node has committed.
	 * @param nowMillis The time of the send.
	 * @return The message which now MUST be sent to the peer.
	 */
	public DownstreamMessage commitToSendMutations(long currentTermNumber, long previousMutationTermNumber, MutationRecord mutation, long lastCommittedMutationOffset, long nowMillis) {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(_original.isConnectionUp);
		Assert.assertTrue(_original.isWritable);
		Assert.assertTrue(_original.didHandshake);
		Assert.assertTrue(_original.nextMutationOffsetToSend == mutation.globalOffset);
		Assert.assertTrue(null == _original.pendingVoteRequest);
		
		DownstreamMessage message = DownstreamMessage.appendMutations(currentTermNumber, previousMutationTermNumber, mutation, lastCommittedMutationOffset);
		_original.isWritable = false;
		_original.lastSentMessageMillis = nowMillis;
		// Clear the next mutation until they ack it.
		_original.nextMutationOffsetToSend = DownstreamPeerState.NO_NEXT_MUTATION;
		_isValid = false;
		return message;
	}

	/**
	 * Called when the caller has decided to send a heart beat to the peer.
	 * Mutative operation which invalidates the receiver.
	 * 
	 * @param currentTermNumber The current term of this node.
	 * @param lastCommittedMutationOffset The last mutation this node has committed.
	 * @param nowMillis The time of the send.
	 * @return The message which now MUST be sent to the peer.
	 */
	public DownstreamMessage commitToSendHeartbeat(long currentTermNumber, long lastCommittedMutationOffset, long nowMillis) {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(_original.isConnectionUp);
		Assert.assertTrue(_original.isWritable);
		Assert.assertTrue(_original.didHandshake);
		Assert.assertTrue(null == _original.pendingVoteRequest);
		
		DownstreamMessage message = DownstreamMessage.heartbeat(currentTermNumber, lastCommittedMutationOffset);
		_original.isWritable = false;
		_original.lastSentMessageMillis = nowMillis;
		_isValid = false;
		return message;
	}

	/**
	 * Called when the caller has decided to send a vote request to the peer.
	 * Mutative operation which invalidates the receiver.
	 * Note that this assumes they already had a pending vote set.
	 * 
	 * @param nowMillis The time of the send.
	 * @return The message which now MUST be sent to the peer.
	 */
	public DownstreamMessage commitToSendVoteRequest(long nowMillis) {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(_original.isConnectionUp);
		Assert.assertTrue(_original.isWritable);
		Assert.assertTrue(_original.didHandshake);
		Assert.assertTrue(null != _original.pendingVoteRequest);
		
		DownstreamMessage message = _original.pendingVoteRequest;
		_original.isWritable = false;
		_original.lastSentMessageMillis = nowMillis;
		_original.pendingVoteRequest = null;
		_isValid = false;
		return message;
	}
}
