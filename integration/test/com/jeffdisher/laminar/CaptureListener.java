package com.jeffdisher.laminar;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;

import com.jeffdisher.laminar.client.ListenerConnection;
import com.jeffdisher.laminar.types.ClusterConfig;
import com.jeffdisher.laminar.types.Consequence;
import com.jeffdisher.laminar.types.TopicName;


/**
 * A testing utility for running a listener in its own thread to capture events from the cluster.
 */
public class CaptureListener extends Thread {
	private final ListenerConnection _listener;
	private final Consequence[] _captured;
	private int _totalEventsConsumed;
	private UUID _configSender;
	private long _configNonce;
	
	public CaptureListener(InetSocketAddress address, TopicName topic, int messagesToCapture) throws IOException {
		_listener = ListenerConnection.open(address, topic, 0L);
		_captured = new Consequence[messagesToCapture];
	}
	
	/**
	 * Used in tests which update the config, since that is a special-case in the nonce order verification since
	 * configs still increment the client nonce but do not generate events which listeners can receive.
	 * 
	 * @param clientId The UUID of the client which updated the config.
	 * @param nonce The nonce of the config update message.
	 */
	public void skipNonceCheck(UUID clientId, long nonce) {
		_configSender = clientId;
		_configNonce = nonce;
	}

	public ClusterConfig getCurrentConfig() {
		return _listener.getCurrentConfig();
	}
	
	public synchronized int waitForEventCount(int count) throws InterruptedException {
		while (_totalEventsConsumed < count) {
			this.wait();
		}
		return _totalEventsConsumed;
	}
	
	public Consequence[] waitForTerminate() throws InterruptedException {
		this.join();
		return _captured;
	}
	
	@Override
	public void run() {
		try {
			Map<UUID, Long> expectedNonceByClient = new HashMap<>();
			long expectedTermNumber = 1L;
			for (int i = 0; i < _captured.length; ++i) {
				_captured[i] = _listener.pollForNextConsequence();
				Assert.assertTrue(_captured[i].termNumber >= expectedTermNumber);
				expectedTermNumber = _captured[i].termNumber;
				Assert.assertEquals(i + 1, _captured[i].consequenceOffset);
				long expectedNonce = 1L;
				if (expectedNonceByClient.containsKey(_captured[i].clientId)) {
					expectedNonce = expectedNonceByClient.get(_captured[i].clientId);
				}
				if (_captured[i].clientId.equals(_configSender) && (expectedNonce == _configNonce)) {
					// We don't see config changes in the listener event stream so skip over this one.
					expectedNonce += 1L;
				}
				Assert.assertEquals(expectedNonce, _captured[i].clientNonce);
				expectedNonceByClient.put(_captured[i].clientId, expectedNonce + 1L);
				synchronized (this) {
					_totalEventsConsumed += 1;
					this.notifyAll();
				}
			}
			_listener.close();
		} catch (IOException | InterruptedException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}
}
