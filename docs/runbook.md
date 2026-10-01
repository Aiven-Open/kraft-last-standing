# Runbook: recovering from lost controller quorum

This runbook describes how to bring an Apache Kafka® cluster back online after its KRaft
controller quorum has permanently lost a majority of its voters. For example, a cluster
with three voters that has irrecoverably lost two of them.

<!-- prettier-ignore-start -->
> [!CAUTION]
> This is a destructive operation that cements data loss. Before continuing, exhaust
> every other option of recovering the lost nodes, or of recovering only their
> `__cluster_metadata-0` directories.
<!-- prettier-ignore-end -->

## Background

Raft, and with it KRaft, needs a majority of voters to make progress. In a dynamic
quorum, the voter set is stored in the metadata log as `VotersRecord` control records,
so removing the lost voters means committing a new `VotersRecord`, which needs the
majority that was lost. The surviving controllers keep waiting for the lost voters, and
the cluster can't create topics, change configuration or elect partition leaders.

The way out is to edit the log of one surviving controller while it is stopped,
appending `VotersRecord` entries that demote every other voter, one at a time. When that
controller starts again, it is the only voter, considers itself leader, and can commit
again. The surviving nodes replicate the appended records, learn that they are no longer
voters, and continue from there.

Losing a majority of voters almost always means losing data, and this procedure doesn't
recover any. Metadata that only existed on the lost nodes is gone, and depending on
replication factors and which replicas survived, so is partition data. The procedure
restores availability, not integrity.

To lose as little as possible, recovery starts from the most progressed surviving copy
of the metadata log, which can belong to a broker as well as to a controller. Brokers
can't handle the metadata log moving backwards, for instance leader epochs decreasing,
so the recovered log must contain every record that a surviving node has already seen.
Everything after the end of that copy is lost for good.

The blog post [Never lose quorum in Apache Kafka®][blog] covers this reasoning in more
depth.

[blog]: https://aiven.io/blog/kafka-never-lose-quorum

## Identification

All of the following must hold before starting.

- The cluster runs in KRaft mode with a dynamic quorum, which requires Kafka 3.9 or
  later. Nodes in a dynamic quorum are configured with
  `controller.quorum.bootstrap.servers` instead of `controller.quorum.voters`.
  `kraft-last-standing probe` lists the voters of a dynamic quorum.
- The metadata log doesn't make progress. The `high-watermark` attribute of the
  `kafka.server:type=raft-metrics` MBean has stopped increasing on all nodes, and
  `kafka-metadata-quorum.sh --bootstrap-controller HOST:PORT describe --status` fails or
  shows no leader.
- The cause is that a majority of the voters is lost, and it's exhaustively verified
  that they can't be recovered.

## Remediation

The procedure, in short:

1. Back up the metadata log of every surviving node.
2. Find the most progressed copy of the metadata log.
3. Stop every controller.
4. Recover one controller, from the most progressed copy.
5. Start only the recovered controller.
6. Start the other surviving nodes.
7. Add controllers until the quorum has its intended size again.

### 1. Back up the metadata log of every surviving node

`kraft-last-standing recover` backs up the directory it modifies, but nothing else.
Before starting, make a copy of the `__cluster_metadata-0` directory of every surviving
broker and controller, so that the state as it was before recovery is available should
something go wrong.

### 2. Find the most progressed copy of the metadata log

Every broker and controller has a local copy of the metadata log, in a directory named
`__cluster_metadata-0`. It lives in the directory configured with `metadata.log.dir`, or
in the first entry of `log.dirs` when that isn't set. A node with
`process.roles=broker,controller` has a single copy, shared by both roles.

Install kraft-last-standing on every surviving node, and probe its copy:

```shell
$ kraft-last-standing probe /var/lib/kafka/data/__cluster_metadata-0
/var/lib/kafka/data/__cluster_metadata-0
  node ID:      1
  last offset:  1621
  leader epoch: 5
  snapshot:     00000000000000001600-0000000005.checkpoint
  voters:       1, 2, 3
```

Probing only reads files. When gathering results programmatically, use `--json` for
machine-readable output.

<!-- prettier-ignore-start -->
> [!WARNING]
> Make sure to probe every surviving node, brokers included. A node that is left out
> might hold the most progressed copy of the log.
<!-- prettier-ignore-end -->

The most progressed copy is the one to recover from. Compare copies the way Raft
compares logs: the copy with the highest leader epoch is the most progressed, and among
copies with the same leader epoch, the one with the highest last offset. A copy can have
a higher last offset but a lower leader epoch than another, when it diverged from the
others under an earlier leader. The records in its divergent tail were never committed.
If several copies are equally progressed, prefer one that belongs to a controller.

- If a controller has the most progressed copy, recover that controller.
- If a broker has the most progressed copy, pick any surviving controller that is listed
  among the voters, and copy the broker's `__cluster_metadata-0` directory to that
  controller's host. Copy it into a new directory, rather than over the controller's
  own. For example:

  ```shell
  $ mkdir /var/tmp/broker-4
  $ scp -r broker-4:/var/lib/kafka/data/__cluster_metadata-0 /var/tmp/broker-4/
  ```

  Nothing appends to the metadata log while the quorum has no leader, so the copy is
  consistent even when the broker is running.

If probing fails with an error about failing to read a record batch, that copy of the
log is corrupt. Leave it out, and recover from the most progressed of the other copies.

### 3. Stop every controller

Stop the Kafka process of every controller, including the one to recover. Also stop
anything that would restart them, such as a systemd unit with `Restart=` set, or
configuration management. When an operator such as Strimzi manages the cluster, pause
its reconciliation, so that it doesn't restart or roll nodes during the procedure.

Brokers can keep running.

### 4. Recover one controller

On the controller chosen in step 2, run `recover` as the user that owns the Kafka data,
or as root:

```shell
$ kraft-last-standing recover \
    --metadata-dir /var/lib/kafka/data/__cluster_metadata-0 \
    --backup-dir /var/backups/kafka
```

When recovering from a broker's copy, also pass the directory it was copied to:

```shell
$ kraft-last-standing recover \
    --metadata-dir /var/lib/kafka/data/__cluster_metadata-0 \
    --backup-dir /var/backups/kafka \
    --copy-log-from /var/tmp/broker-4/__cluster_metadata-0
```

The command reads the controller's node ID and directory ID from `meta.properties`,
prints a plan, and asks for confirmation before modifying anything. Check that the plan
keeps the intended voter, and that the last offset matches the one found in step 2. The
[usage] page describes the options in detail.

[usage]: usage.md

### 5. Start only the recovered controller

Start the Kafka process of the recovered controller, and keep every other controller
stopped. Verify that it is the only voter, and that it is the leader:

```shell
$ kafka-metadata-quorum.sh --bootstrap-controller controller-1:9093 describe --status
```

`LeaderId` should be the recovered controller's node ID, and `CurrentVoters` should only
list that controller. The high watermark should increase again. Running brokers
reconnect to the new leader, and show up as observers in the output of
`describe --replication`.

### 6. Start the other surviving nodes

Start the remaining surviving nodes, and undo whatever was paused in step 3. Surviving
controllers that were demoted rejoin as observers.

### 7. Restore the size of the quorum

A single voter can't tolerate any failure, so add controllers until the quorum has its
intended size again. Each new controller must be formatted without an initial voter set:

```shell
$ kafka-storage.sh format \
    --cluster-id CLUSTER_ID \
    --no-initial-controllers \
    --config controller.properties
```

The cluster ID is the `cluster.id` in `meta.properties` of any existing node. Start the
new controller, and once it shows up as an observer, promote it to voter by running this
on its host:

```shell
$ kafka-metadata-quorum.sh \
    --bootstrap-controller controller-1:9093 \
    --command-config controller.properties \
    add-controller
```

`add-controller` reads the node ID, the advertised listeners and the directory ID of the
new controller from its configuration and `meta.properties`. The request times out until
the new controller has caught up with the leader, so retry it until it succeeds.

Demoted controllers that survived can be promoted the same way, without formatting them
again.

A lost voter can't be brought back by formatting new storage under its node ID. Kafka
identifies each replica by its node ID together with the directory ID that formatting
generates, so fresh storage is a new replica, even with the same node ID. Now that the
lost voters are no longer in the voter set, a replacement node can reuse their node IDs,
and join as a new voter with `add-controller`.

## Notes

### How recovery works

`recover` reads the log segments and the latest snapshot of the metadata directory, and
combines them the way Kafka does on startup. It finds the last offset, the leader epoch
of the last batch, and the most recent `VotersRecord`, which only exists in the snapshot
once Kafka has deleted the segments that held it. It then writes one `VotersRecord`
control batch per voter to remove, at the offsets following the last one, to a new log
segment named after its first offset, like Kafka names the segments it rolls. Each
record removes one voter, so every step changes the voter set by a single voter, like
regular membership changes do.

### Rolling back

The backup made by `recover` is a directory holding a copy of the metadata directory as
it was before recovery, such as
`/var/backups/kafka/__cluster_metadata-0.20261001T144217Z/__cluster_metadata-0`. To roll
back, stop the controller, and replace its `__cluster_metadata-0` directory with that
copy. The copy has the modes of the original files, but is owned by the user that ran
`recover`, so give it back to the user that runs Kafka when that was root.
