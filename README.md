# Laminar

Experimental programmable event store.  See the wiki for more details of [what and why this is](https://github.com/jmdisher/Laminar/wiki/The-What-and-Why-of-Laminar).

## How to build

The project doesn't yet have any examples but it can be built and tested using the top-level default `ant` target:

```
ant
```

## What is the state of the codebase?

So far, Laminar has been developed through some combination of prototyping a walking skeleton and mad-hackery to sprint to the point where its core thesis can be demonstrated.  As a result, the ability to follow much of the code has suffered.  This will improve in time as future development moves into new parts of the project and the older parts are refined to support these.

Additionally, there are a few bugs within the system which are being addressed.

## Is this production ready?

NO!  This is still an early prototype and is missing core components (all "storage" is an asynchronous in-memory abstraction, for example).  It may eventually become production ready but the goal is currently to demonstrate the core idea.

## Why no issue tracker?

The issue tracker is currently disabled during these early phases of development.  The main reason for this is that parts of the project will eventually be split off as upstream dependencies and GitHub issue references don't do well when that happens.

Additionally, it is currently being developed by 1 person and has no users so it is unlikely the issues would be relevant in the near-term.

As the project progresses, the issue tracker will likely be enabled.
