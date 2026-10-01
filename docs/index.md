# kraft-last-standing

kraft-last-standing recovers an Apache Kafka® cluster whose KRaft controller quorum has
permanently lost a majority of its voters. It shrinks the voter set down to a single
surviving controller, by appending `VotersRecord` control records to that controller's
local copy of the metadata log while it is stopped. When the controller starts again, it
is the only voter, elects itself leader, and the cluster can make progress again.

<!-- prettier-ignore-start -->
> [!CAUTION]
> Recovery cements data loss. Anything that only existed on the lost nodes is gone, and
> this tool cannot bring it back. Only use it once it is verified that the lost voters
> cannot be recovered, and follow the [runbook].
<!-- prettier-ignore-end -->

[runbook]: runbook.md

## Background

Since [KIP-853], the set of voters in a dynamic KRaft quorum lives in the metadata log
itself, as `VotersRecord` control records. Changing the voter set means committing a new
`VotersRecord`, which requires a majority of the current voters. When a majority is
permanently lost, the lost voters can only be removed with quorum, and quorum can only
be restored by removing the lost voters. Stock Kafka tooling has no way out of that
loop, so the cluster stays down.

[KIP-853]:
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-853%3A+KRaft+Controller+Membership+Changes

The way out is an offline edit of the log, in the same family as formatting storage with
an initial voter set. The blog post [Never lose quorum in Apache Kafka®][blog] describes
the approach and its consequences in depth.

[blog]: https://aiven.io/blog/kafka-never-lose-quorum

## Installation

```shell
$ pip install --require-virtualenv kraft-last-standing
```

kraft-last-standing requires Python 3.12 or later, and runs on the hosts that store the
metadata log, reading and writing its files directly. It doesn't connect to Kafka.

## Overview

The tool has two commands, which the [runbook] puts in context.

- `kraft-last-standing probe` reports how far local copies of the metadata log have
  progressed, so that recovery can start from the most progressed one. It only reads.
- `kraft-last-standing recover` backs up the metadata log of a stopped controller, and
  then appends records that remove every other voter, one at a time.

The [usage] page documents both commands in detail.

[usage]: usage.md

## Compatibility

kraft-last-standing works with clusters on Kafka 3.9 or later that use a dynamic quorum,
that is with `kraft.version` 1 or later. It is tested against Kafka 4.3.1. A static
quorum has no `VotersRecord` in its log, so the tool refuses to modify it.
