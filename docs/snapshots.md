# Snapshots ahead of the log

Without `--copy-log-from`, `recover` only deletes log segments when the latest snapshot
is ahead of the log, which happens when a controller crashes right after downloading a
snapshot from the leader. Kafka deletes such segments on its next start. For Kafka to
keep the batches the tool appends, the tool must delete the segments first, so that it
can write the batches at the end of the snapshot, where Kafka continues the log. With
the segments in place, batches written after them are deleted along with them when Kafka
starts, and batches written at the end of the snapshot leave a gap in the log, which
Kafka never produces.

## How a controller gets there

Controller 2 was offline for ten days. Meanwhile, the leader deleted old segments, and
its log now starts at offset 9000. Controller 2 comes back and fetches from offset 4200,
which the leader no longer has, so the leader answers with the ID of its latest
snapshot, which holds the state up to offset 9499 in epoch 12. Controller 2 downloads
the snapshot, renames it into place, and is killed before it deletes its own segments.
Its metadata directory now holds:

```text
00000000000000003000.log                    offsets 3000–3599
00000000000000003600.log                    offsets 3600–4199, last epoch 9
00000000000000009500-0000000012.checkpoint  state up to offset 9499, epoch 12
```

A follower renames a downloaded snapshot into place before it deletes its segments
([`KafkaRaftClient`][download]), and Kafka's startup code [handles a crash in
between][startup]:

> When recovering, truncate fully if the latest snapshot is after the log end offset.
> This can happen to a follower when the follower crashes after downloading a snapshot
> from the leader but before it could truncate the log fully.

[download]:
  https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L2181-L2186
[startup]:
  https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java#L744-L746

## What Kafka does on startup

A snapshot is ahead of the log when its epoch is higher than that of the last batch in
the log, or the epochs are equal and the snapshot ends after the log does
([`truncateToLatestSnapshot`][truncate]). When it is, Kafka calls
[`truncateFullyAndStartAt`][truncate-fully] with the end offset of the snapshot, which
deletes every segment, clears the leader epoch cache, and starts an empty segment at
that offset. In the example, Kafka deletes both segments and continues the log at
offset 9500.

[truncate]:
  https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java#L244-L264
[truncate-fully]:
  https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java#L2387-L2402

The tool does the same before appending. It deletes the segments and
`leader-epoch-checkpoint`, which Kafka rebuilds from the log, and writes the new batches
at the end offset of the snapshot, in the epoch of the snapshot. The plan lists the
deleted segments under `segments to delete`, and the backup taken before any change
holds them.

This differs from the usual state, where a snapshot covers a prefix of the segments.
Kafka only takes snapshots of offsets it has, and deletes the segments behind them
later, once they exceed the retention settings. The tool deletes nothing in that state.

## Copying the log

With `--copy-log-from`, the copied directory can be in the same state. The tool then
leaves the old segments out of the copy, lists them under `segments to skip`, and
deletes nothing from the source. It replaces the controller's own segments either way,
which is what the option is for.

## How common it is

A leader sends a snapshot to any follower that fetches from before the leader's log
start offset, or with a last fetched epoch older than the leader's oldest snapshot
([`RaftLog`][fetch-behind]). It also sends one to every follower that fetches from
offset 0, once it has a snapshot other than the bootstrap one
([`KafkaRaftClient`][fetch-empty]). Snapshot downloads are routine, but ending up in
this state also takes a crash between the rename and the deletion, a window of a few
file operations.

[fetch-behind]:
  https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/RaftLog.java#L97-L121
[fetch-empty]:
  https://github.com/apache/kafka/blob/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L1621-L1624
