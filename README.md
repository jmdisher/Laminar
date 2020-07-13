# Laminar

Experimental programmable event store.  See the wiki for more details of [what and why this is](https://github.com/jmdisher/Laminar/wiki/The-What-and-Why-of-Laminar).

Further discussion of the progress and design thoughts can be found on the [Laminar Blog](https://jmdisher.github.io/Laminar-blog/).


## How to start the server and form a cluster

Download `Laminar.jar` from [Laminar releases](https://github.com/jmdisher/Laminar/releases/tag/0.0-research1).

To start the server, you must specify the IP which can be used by clients, other servers in a cluster, their respective TCP ports, and a directory for the data storage:

```
java -jar Laminar.jar --clientIp 127.0.0.1 --clientPort 8000 --clusterIp 127.0.0.1 --clusterPort 9000 --data /data/storage/directory
```

This client IP and port can be used by client applications when connecting to the server.

To start a cluster, also download `ConfigBuilder.jar` from [Laminar releases](https://github.com/jmdisher/Laminar/releases/tag/0.0-research1).

First, start both servers (we will start them both on localhost, for this example):

```
java -jar Laminar.jar --clientIp 127.0.0.1 --clientPort 8000 --clusterIp 127.0.0.1 --clusterPort 9000 --data /tmp/laminar1
java -jar Laminar.jar --clientIp 127.0.0.1 --clientPort 8001 --clusterIp 127.0.0.1 --clusterPort 9001 --data /tmp/laminar2
```

Once they both start up, `ConfigBuilder` can be run to put them into a cluster by providing the **client** IP and port pairs used to configure each server:

```
java -jar ConfigBuilder.jar 127.0.0.1 8000 127.0.0.1 8001
```

The client IP and port combination from any of the servers can be used by client applications wishing to connect to the cluster (they will find the leader and config for fail-over, automatically).

More detailed information can be found by adding `--verbose` to the command-line but little of this is of value for non-developer usage.


## How to get the client library

Connecting to a Laminar cluster can be done using the client library (found along with the server in the release) or as a Maven resource:

```
	<repositories>
		<repository>
			<id>laminar-repo</id>
			<url>https://github.com/jmdisher/Laminar/raw/maven/repo</url>
		</repository>
	</repositories>
	<dependencies>
		<dependency>
			<groupId>com.jeffdisher.laminar</groupId>
			<artifactId>clientlib</artifactId>
			<version>0.0-research1</version>
		</dependency>
	</dependencies>
```

Connecting to the cluster for write operations can be done via `ClientConnection` while connecting for reading operations can be done with `ListenerConnection`.


## How to build

The top-level `ant` target will run a clean build and test of the entire project:

```
ant
```


## Performance runs

Once the build has been created, a basic performance run can be started from the top-level directory (takes about 3x as long as build and test):

```
ant perf
```

This produces CSV snippets of results of the various performance tests as it runs (as part of the JUnit output).


## What is the state of the codebase?

The codebase is now to the point where it is generally well organized and of reasonable quality.  That said, it still isn't production quality as there are remaining TODOs and *unreachable* assert cases.  To be production-quality, these cases would all need to be mapped to handled error cases or converted into more concrete assertions around impossible states.  Additionally, there is still some duplication which could be coalesced to avoid divergence through future changes.  Testing around certain limit cases also needs to be further fleshed-out (most testing has been to prove *happy-path* feature support as opposed to stress-testing corner cases for hardening).  Finally, further testing in various network topologies needs to be done before this could be used in a production environment (as most testing as been done on one system, just to prove out feature completeness - this means there may be timing or failure detection cases not properly handled).

That said, the core implementation is complete so all baseline features are supported.  The one feature which would be conspicuously absent in a long-running production deployment is log compaction.  This was not within scope for the research initiative so on-disk storage and new node synchronization time can grow unbounded.  If there is an interest in driving this to a production-level product, this will likely be the first feature added.


## Is this production ready?

Not at this time.  At this point, it could be considered ready for experimentation or evaluation but should not be used in production.

So far, the main goal has been to demonstrate the core ideas for discussion in the research paper which means that fleshing out corner cases and correctness hardening haven't been priorities.

There are currently no plans to push this forward to a production-ready release but that could change if there were more interest.


## Why no issue tracker?

The issue tracker is currently disabled during these early phases of development.  The main reason for this is that parts of the project will eventually be split off as upstream dependencies and GitHub issue references don't do well when that happens.

Additionally, it is currently being developed by 1 person and has no users so it is unlikely the issues would be relevant in the near-term.

As the project progresses, the issue tracker will likely be enabled.
