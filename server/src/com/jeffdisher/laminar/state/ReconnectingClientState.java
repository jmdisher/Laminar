package com.jeffdisher.laminar.state;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.types.ClientResponse;


/**
 * The state of the ongoing reconnection associated with the client connected with token.
 * This is updated by NodeState until the resyncing of the attached client is completed.
 */
public class ReconnectingClientState {
	public final ClientManager.ClientNode token;
	public final List<ClientResponse> outgoingMessages = new LinkedList<>();
	public boolean writable = true;
	public final UUID clientId;
	public long earliestNextNonce;
	public long lastCheckedGlobalOffset;
	public final long finalGlobalOffsetToCheck;

	public ReconnectingClientState(ClientManager.ClientNode token, UUID clientId, long earliestNextNonce, long lastCheckedGlobalOffset, long finalGlobalOffsetToCheck) {
		this.token = token;
		this.clientId = clientId;
		this.earliestNextNonce = earliestNextNonce;
		this.lastCheckedGlobalOffset = lastCheckedGlobalOffset;
		this.finalGlobalOffsetToCheck = finalGlobalOffsetToCheck;
	}
}
