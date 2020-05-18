package com.jeffdisher.laminar.network;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.jeffdisher.laminar.components.NetworkManager;
import com.jeffdisher.laminar.types.CommitInfo;
import com.jeffdisher.laminar.types.response.ClientResponse;


/**
 * The information required to track the state of a single connected client.
 * This only tracks information related to normal client state.  New clients have no state and listeners use
 * ListenerState.
 */
public class ClientState {
	public final List<ClientResponse> outgoingMessages = new LinkedList<>();
	public boolean writable = true;
	public final UUID clientId;
	public final NetworkManager.NodeToken token;
	public long nextNonce;
	/**
	 * The list of nonces committed while the reconnect was in-process.  Ideally, we would just cache the outgoing
	 * messages but they will contain a stale globalCommitOffset for the server (since it typically will have advanced
	 * since then) so we just record the nonces and synthesize the commits when the reconnect is done.
	 */
	public List<Long> noncesCommittedDuringReconnect;
	/**
	 * We also store the commit info of the commits we observed during reconnect.
	 */
	public List<CommitInfo> correspondingCommitInfo;

	/**
	 * Creates a new normal client state with the given clientId and nextNonce.  The nonce is typically set to 1L but
	 * this is left open for future reconnect logic where a client may want to tell us its nonce.
	 * 
	 * @param clientId The UUID of this client.
	 * @param token The token for communicating with this client.
	 * @param nextNonce The next nonce the client is expected to send.
	 */
	public ClientState(UUID clientId, NetworkManager.NodeToken token, long nextNonce) {
		this.clientId = clientId;
		this.token = token;
		this.nextNonce = nextNonce;
	}
}
