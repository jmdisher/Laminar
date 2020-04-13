package com.jeffdisher.laminar.state;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.jeffdisher.laminar.network.ClientResponse;


/**
 * The information required to track the state of a single connected client.
 * Note that we track all of new clients, normal clients, and listeners using these structures.
 * Only a normal client will have a non-null clientId while both new clients and listeners will have null.
 */
public class ClientState {
	public final List<ClientResponse> outgoingMessages = new LinkedList<>();
	public boolean writable = true;
	public int readableMessages = 0;
	public UUID clientId = null;
	public long nextNonce = 0;
	// For listeners, this is the most recently sent local offset event.
	public long lastSentLocalOffset = 0;
}
