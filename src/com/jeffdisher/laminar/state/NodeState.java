package com.jeffdisher.laminar.state;

import com.jeffdisher.laminar.console.ConsoleManager;
import com.jeffdisher.laminar.console.IConsoleManagerBackgroundCallbacks;
import com.jeffdisher.laminar.disk.DiskManager;
import com.jeffdisher.laminar.disk.IDiskManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.ClientManager;
import com.jeffdisher.laminar.network.ClusterManager;
import com.jeffdisher.laminar.network.IClientManagerBackgroundCallbacks;
import com.jeffdisher.laminar.network.IClusterManagerBackgroundCallbacks;
import com.jeffdisher.laminar.utils.Assert;


/**
 * Maintains the state of this specific node.
 * Primarily, this is where the main coordination thread sleeps until events are handed off to it.
 * Note that this "main thread" is actually the thread which started executing the program.  It is not started here.
 * Note that the thread which creates this instance is defined as "main" and MUST be the same thread which calls
 * runUntilShutdown() and MUST NOT call any background* methods (this is to verify re-entrance safety, etc).
 */
public class NodeState implements IClientManagerBackgroundCallbacks, IClusterManagerBackgroundCallbacks, IDiskManagerBackgroundCallbacks, IConsoleManagerBackgroundCallbacks {
	// We keep the main thread for asserting no re-entrance bugs or invalid interface uses.
	private final Thread _mainThread;

	private ClientManager _clientManager;
	private ClusterManager _clusterManager;
	private DiskManager _diskManager;
	private ConsoleManager _consoleManager;

	private RaftState _currentState;
	private boolean _keepRunning;

	public NodeState() {
		// We define the thread which instantiates us as "main".
		_mainThread = Thread.currentThread();
		// Note that we default to the LEADER state (typically forced into a FOLLOWER state when an existing LEADER attempts to append entries).
		_currentState = RaftState.LEADER;
	}

	public synchronized void runUntilShutdown() {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Not fully configuring the instance is a programming error.
		Assert.assertTrue(null != _clientManager);
		Assert.assertTrue(null != _clusterManager);
		Assert.assertTrue(null != _diskManager);
		Assert.assertTrue(null != _consoleManager);
		
		// For now, we will just wait until we get a "stop" command from the console.
		_keepRunning = true;
		while (_keepRunning) {
			try {
				this.wait();
			} catch (InterruptedException e) {
				// We don't use interruption.
				Assert.unexpected(e);
			}
		}
	}

	public void registerClientManager(ClientManager clientManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clientManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clientManager);
		_clientManager = clientManager;
	}

	public void registerClusterManager(ClusterManager clusterManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != clusterManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _clusterManager);
		_clusterManager = clusterManager;
		
	}

	public void registerDiskManager(DiskManager diskManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != diskManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _diskManager);
		_diskManager = diskManager;
	}

	public void registerConsoleManager(ConsoleManager consoleManager) {
		// This MUST be called on the main thread.
		Assert.assertTrue(Thread.currentThread() == _mainThread);
		// Input CANNOT be null.
		Assert.assertTrue(null != consoleManager);
		// Reconfiguration is not defined.
		Assert.assertTrue(null == _consoleManager);
		_consoleManager = consoleManager;
	}

	// <IClientManagerBackgroundCallbacks>
	@Override
	public void clientConnectedToUs(ClientManager.ClientNode node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientDisconnectedFromUs(ClientManager.ClientNode node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientWriteReady(ClientManager.ClientNode node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clientReadReady(ClientManager.ClientNode node) {
		// TODO Auto-generated method stub
		
	}
	// </IClientManagerBackgroundCallbacks>

	// <IClusterManagerBackgroundCallbacks>
	@Override
	public void peerConnectedToUs(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void peerDisconnectedFromUs(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void peerWriteReady(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void peerReadReady(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void weConnectedToPeer(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void weDisconnectedFromPeer(ClusterManager.ClusterNode realNode) {
		// TODO Auto-generated method stub
		
	}
	// </IClusterManagerBackgroundCallbacks>

	// <IConsoleManagerBackgroundCallbacks>
	@Override
	public synchronized void handleStopCommand() {
		// This MUST NOT be called on the main thread.
		Assert.assertTrue(Thread.currentThread() != _mainThread);
		
		_keepRunning = false;
		this.notifyAll();
	}
	// </IConsoleManagerBackgroundCallbacks>
}
