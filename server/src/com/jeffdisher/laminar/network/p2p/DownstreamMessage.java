package com.jeffdisher.laminar.network.p2p;

import java.nio.ByteBuffer;

import com.jeffdisher.laminar.types.ConfigEntry;
import com.jeffdisher.laminar.types.mutation.MutationRecord;
import com.jeffdisher.laminar.utils.Assert;


/**
 * An instance of a message along the pear-to-peer layer.  These are meant to be sent on the node's "outgoing"
 * connections.  The target will respond with an UpstreamResponse, sent along the same socket.
 */
public class DownstreamMessage {
	public static DownstreamMessage identity(ConfigEntry self) {
		return new DownstreamMessage(Type.IDENTITY, DownstreamPayload_Identity.create(self));
	}

	public static DownstreamMessage appendMutations(long termNumber, long previousMutationTermNumber, MutationRecord mutation, long lastCommittedMutationOffset) {
		return new DownstreamMessage(Type.APPEND_MUTATIONS, DownstreamPayload_AppendMutations.create(termNumber, previousMutationTermNumber, new MutationRecord[] { mutation }, lastCommittedMutationOffset));
	}

	public static DownstreamMessage heartbeat(long termNumber, long lastCommittedMutationOffset) {
		return new DownstreamMessage(Type.APPEND_MUTATIONS, DownstreamPayload_AppendMutations.create(termNumber, 0L, new MutationRecord[0], lastCommittedMutationOffset));
	}

	public static DownstreamMessage requestVotes(long newTermNumber, long previousMutationTerm, long previousMuationOffset) {
		return new DownstreamMessage(Type.REQUEST_VOTES, DownstreamPayload_RequestVotes.create(newTermNumber, previousMutationTerm, previousMuationOffset));
	}

	public static DownstreamMessage deserializeFrom(ByteBuffer buffer) {
		byte typeByte = buffer.get();
		if ((typeByte < 0) || (typeByte >= Type.values().length)) {
			throw _parseError();
		}
		Type type = Type.values()[typeByte];
		IDownstreamPayload payload;
		switch (type) {
		case IDENTITY:
			payload = DownstreamPayload_Identity.deserializeFrom(buffer);
			break;
		case APPEND_MUTATIONS:
			payload = DownstreamPayload_AppendMutations.deserializeFrom(buffer);
			break;
		case REQUEST_VOTES:
			payload = DownstreamPayload_RequestVotes.deserializeFrom(buffer);
			break;
		case INVALID:
			throw _parseError();
		default:
			throw Assert.unreachable("Case not handled");
		}
		return new DownstreamMessage(type, payload);
	}


	private static IllegalArgumentException _parseError() {
		throw new IllegalArgumentException("DownstreamMessage invalid");
	}


	public final Type type;
	public final IDownstreamPayload payload;
	
	private DownstreamMessage(Type type, IDownstreamPayload payload) {
		this.type = type;
		this.payload = payload;
	}

	public int serializedSize() {
		return Byte.BYTES + this.payload.serializedSize();
	}

	public void serializeInto(ByteBuffer buffer) {
		buffer.put((byte)this.type.ordinal());
		this.payload.serializeInto(buffer);
	}

	@Override
	public String toString() {
		return "DownstreamMessage(type=" + this.type + ", payload=" + this.payload + ")";
	}


	public static enum Type {
		INVALID,
		IDENTITY,
		APPEND_MUTATIONS,
		REQUEST_VOTES,
	}
}
