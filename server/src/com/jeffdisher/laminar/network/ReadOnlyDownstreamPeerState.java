package com.jeffdisher.laminar.network;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.network.p2p.DownstreamMessage;
import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.Intention;
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
	 * @return True if the peer has a intention it could send.
	 */
	public boolean hasIntentionToSend() {
		Assert.assertTrue(_isValid);
		return (DownstreamPeerState.NO_NEXT_INTENTION != _original.nextIntentionOffsetToSend);
	}

	/**
	 * @return True if the peer has a vote is could send.
	 */
	public boolean hasVoteToSend() {
		Assert.assertTrue(_isValid);
		return (null != _original.pendingVoteRequest);
	}

	/**
	 * @return The next intention offset the receiver could send.
	 */
	public long getNextIntentionOffsetToSend() {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(DownstreamPeerState.NO_NEXT_INTENTION != _original.nextIntentionOffsetToSend);
		return _original.nextIntentionOffsetToSend;
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
	 * Called when the caller has decided to send an intention to the peer.
	 * Mutative operation which invalidates the receiver.
	 * Note that this assumes the receiver was already ready to send this intention.
	 * 
	 * @param currentTermNumber The current term of this node.
	 * @param previousIntentionTermNumber The term number of the intention before this one.
	 * @param intention The intention to send.
	 * @param lastCommittedIntentionOffset The last intention this node has committed.
	 * @param nowMillis The time of the send.
	 * @return The message which now MUST be sent to the peer.
	 */
	public DownstreamMessage commitToSendIntentions(long currentTermNumber, long previousIntentionTermNumber, Intention intention, long lastCommittedIntentionOffset, long nowMillis) {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(_original.isConnectionUp);
		Assert.assertTrue(_original.isWritable);
		Assert.assertTrue(_original.didHandshake);
		Assert.assertTrue(_original.nextIntentionOffsetToSend == intention.intentionOffset);
		Assert.assertTrue(null == _original.pendingVoteRequest);
		
		DownstreamMessage message = DownstreamMessage.appendIntentions(currentTermNumber, previousIntentionTermNumber, intention, lastCommittedIntentionOffset);
		_original.isWritable = false;
		_original.lastSentMessageMillis = nowMillis;
		// Clear the next intention until they ack it.
		_original.nextIntentionOffsetToSend = DownstreamPeerState.NO_NEXT_INTENTION;
		_isValid = false;
		return message;
	}

	/**
	 * Called when the caller has decided to send a heart beat to the peer.
	 * Mutative operation which invalidates the receiver.
	 * 
	 * @param currentTermNumber The current term of this node.
	 * @param lastCommittedIntentionOffset The last intention this node has committed.
	 * @param nowMillis The time of the send.
	 * @return The message which now MUST be sent to the peer.
	 */
	public DownstreamMessage commitToSendHeartbeat(long currentTermNumber, long lastCommittedIntentionOffset, long nowMillis) {
		Assert.assertTrue(_isValid);
		Assert.assertTrue(_original.isConnectionUp);
		Assert.assertTrue(_original.isWritable);
		Assert.assertTrue(_original.didHandshake);
		Assert.assertTrue(null == _original.pendingVoteRequest);
		
		DownstreamMessage message = DownstreamMessage.heartbeat(currentTermNumber, lastCommittedIntentionOffset);
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
