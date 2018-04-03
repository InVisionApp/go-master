[![Build Status](https://travis-ci.com/InVisionApp/go-background.svg?token=KosA43m1X3ikri8JEukQ&branch=master)](https://travis-ci.com/InVisionApp/go-background)

# go-master
A simple framework for master selection using flexible backends. With the proliferation of micro-services, there are often cases where a clustered service that already has a shared backend resource will need to implement master selection. This library provides a simple way to enable master selection in a clustered service with a shared backend. It also includes a set of swappable lock implementations for common backends.  
Unlike more complex leader election mechanisms based on consensus algorithms, this simply uses a lock on a shared resource. In an environment of micro-services running in a docker orchestration platform it can often be difficult to obtain direct communication channels between all the nodes of a service, and many services will already require a backend database to store shared state. This master selection library is designed to to cover those use cases.

## Master Selection
<img align="right" src="images/master_process.svg" width="300">
There is a single master at all times. The master is chosen using the master lock backend. All nodes will attempt to become a master, but only one will succeed. The node chosen to be the master will write a heartbeat to the master lock while it is healthy and executing its duties. 

<br><br><br><br><br><br><br><br><br><br>

## Supported Backends
This library comes with support for a set of backend databases that are commonly used. All backend implementations can be found in `go-master/backend`, each under their own packages.
