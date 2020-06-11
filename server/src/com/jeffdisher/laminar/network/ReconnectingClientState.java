package com.jeffdisher.laminar.network;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.response.ClientResponse;


/**
 * The state of the ongoing reconnection associated with the client connected with token.
 * This is updated by NodeState until the resyncing of the attached client is completed.
 */
public class ReconnectingClientState {
	public final NetworkManager.NodeToken token;
	public final List<ClientResponse> outgoingMessages = new LinkedList<>();
	public boolean writable = true;
	public final UUID clientId;
	/**
	 * Initially given to the server as the nonce of the first message it didn't see commit.
	 * As the reconnect progresses, this value is incremented for every received message sent back to the client.
	 * At the end, we give this back to the client as the first thing we didn't see (and therefore didn't send as a
	 * received).  They then use this to determine the first thing to send as a fresh message, once the reconnect is
	 * done.
	 */
	public long earliestNextNonce;
	/**
	 * Initially set as the last commit the client knew that the server had committed, prior to its disconnect.
	 * This means that the client knows no reconnect data can be from this mutation or earlier, so the server will start
	 * looking for mutations which match this clientId with the mutation after this.
	 */
	public final long lastCheckedIntentionOffset;
	/**
	 * Initialized to the lastCheckedIntentionOffset (since we know that the client received this value) but then updated
	 * as COMMITTED messages are sent to the client, as the reconnect progresses.  This is updated to the offset of a
	 * mutation's offset if its COMMITTED is to be sent, but the value is left as is for RECEIVED-only responses.
	 */
	public long mostRecentlySentServerCommitOffset;
	/**
	 * This represents the end-point the server will use to determine when the reconnect is done.  This is initialized
	 * to the most recent mutation it has received.
	 * This stops the server from continuing to search through mutations which arrived after the reconnect started (as
	 * it is impossible for those to come from this client).
	 */
	public final long finalIntentionOffsetToCheck;
	/**
	 * This is used to avoid sending a commit for something we found in reconnect but also committed during the
	 * reconnect, therefore already queueing up a committed ack to be sent out after the reconnect completes.  This
	 * avoids sending that message twice.
	 * Messages with offsets > this value but <= finalIntentionOffsetToCheck will still generate received acks, though.
	 */
	public final long finalCommitToReturnInReconnect;

	public ReconnectingClientState(NetworkManager.NodeToken token, UUID clientId, long earliestNextNonce, long lastCheckedIntentionOffset, long finalIntentionOffsetToCheck, long finalCommitToReturnInReconnect) {
		this.token = token;
		this.clientId = clientId;
		this.earliestNextNonce = earliestNextNonce;
		this.lastCheckedIntentionOffset = lastCheckedIntentionOffset;
		this.mostRecentlySentServerCommitOffset = lastCheckedIntentionOffset;
		this.finalIntentionOffsetToCheck = finalIntentionOffsetToCheck;
		this.finalCommitToReturnInReconnect = finalCommitToReturnInReconnect;
	}
}
