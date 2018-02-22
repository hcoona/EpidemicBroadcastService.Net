# Epidemic Broadcast Service #

This project provide a service to establish an epidemic-style broadcast infrastructure.
The service is supported by Google [Protocol Buffers](https://developers.google.com/protocol-buffers/) & [gRPC](https://grpc.io/).

## Basic Idea ##

Each rumor $r$ could be in 4 states:

1. A: Unknown, whose counter is 0
1. B: Pushing, whose counter is in range $[1, \text{ctr}_{push}]$, where $\text{ctr}_{push} = \mathcal{O}(\ln\ln n)$
1. C: Pulling, whose counter is in range $[\text{ctr}_{push}, \text{ctr}_{push} + \text{ctr}_{pull}]$, where $\text{ctr}_{pull} = \mathcal{O}(\ln\ln n)$
1. D: Died, whose counter is beyond $\text{ctr}_{max}$

With a configurable interval, the time is split into multiple rounds. A node works as following:

1. Calculate the rumor counters in last round
1. Send rumors & update related counters (only send rumors in B state)
1. Receive & reply pull requests & update related counters (only reply rumors in C state)

The rumor counter is calculated in following rules: (The median-counter algorithm in [1])

Say node $v$ holds a counter for rumor $r$ as $\text{ctr}(v, r)$, node $v$ is in state B-$m$ if $\text{ctr}(v, r) = m$.

* State A.
  * If $v$ in state A receives $r$ only from nodes in state B, then it switches to state B-1.
  * If $v$ in state A receives $r$ from nodes in state C, then it switches to state C.
* State B.
  * If during a round $v$ in state B-$m$ receives $r$ from more players in state B-$m'$ with $m' \ge m$ than from nodes in state A and B-$m''$ with $m'' \lt m$, then it switches to state B-$(m + 1)$.
  * If $v$ receives $r$ from nodes in state C, then it switches to state C.
* State C. Increase counter every round.
* State D. Terminate state.

With this algorithm, we can broadcast a rumor to $n$ nodes with $\mathcal{O}(\ln n)$ rounds and $\mathcal{O}(n)$ messages w.h.p.

## Options ##

* interval: The round interval.
* $\text{fan}_{out}$: The maximum number of nodes the push request sending to
* $\text{fan}_{in}$: The maximum number of nodes the pull request sending to
* $n$: The estimated number of nodes in whole cluster
* $\text{factor}_{push}$: $\text{ctr}_{push} = \text{factor}_{push} \ln\ln n$
* $\text{factor}_{pull}$: $\text{ctr}_{pull} = \text{factor}_{pull} \ln\ln n$

## Future ##

Give a tool like [Serf Convergence Simulator](https://www.serf.io/docs/internals/simulator.html) to give a visible chart about the broadcast time & message load.

With extra parameters:

* Packet loss
* Node failure rate

## References ##

1. R. Karp, C. Schindelhauer, S. Shenker, and B. Vocking, “Randomized rumor spreading,” Proc. 41st Annu. Symp. Found. Comput. Sci., pp. 565–574, 2000.
1. B. Doerr and A. Kostrygin, “Randomized rumor spreading revisited,” in Leibniz International Proceedings in Informatics, LIPIcs, 2017, vol. 80.
1. H. Mercier, L. Hayez, and M. Matos, “Brief Announcement: Optimal Address-Oblivious Epidemic Dissemination,” in Proceedings of the ACM Symposium on Principles of Distributed Computing - PODC ’17, 2017, pp. 151–153.
