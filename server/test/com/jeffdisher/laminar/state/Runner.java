package com.jeffdisher.laminar.state;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Assert;


/**
 * A utility type to make testing NodeState easier.  It is specifically intended to wrap commands being sent to the
 * command queue in such a way that the caller to synchronously interact with the result, even though it needs to pass
 * between threads.
 */
public class Runner {
	private final NodeState _nodeState;

	public Runner(NodeState nodeState) {
		_nodeState = nodeState;
	}

	public <T> T run(Function<StateSnapshot, T> command) {
		TestCommand<T> test = new TestCommand<>(command);
		_nodeState.testEnqueueMessage(test);
		return test.result();
	}


	private static class TestCommand<T> implements Consumer<StateSnapshot> {
		private final CountDownLatch _latch;
		private final Function<StateSnapshot, T> _command;
		private T _result;
		private TestCommand(Function<StateSnapshot, T> command) {
			_latch = new CountDownLatch(1);
			_command = command;
		}
		@Override
		public void accept(StateSnapshot t) {
			_result = _command.apply(t);
			_latch.countDown();
		}
		public T result() {
			try {
				_latch.await();
			} catch (InterruptedException e) {
				Assert.fail("Not used in test");
			}
			return _result;
		}
	}
}
