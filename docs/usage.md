# Usage

Both commands take paths to `__cluster_metadata-0` directories, the directory that holds
a node's local copy of the metadata log. It lives in the directory configured with
`metadata.log.dir`, or in the first entry of `log.dirs` when that isn't set. Brokers and
controllers both have one, and a node with `process.roles=broker,controller` has a
single copy shared by both roles.

Like Kafka, the commands combine the log segments with the latest snapshot. They read
the voter set from the log, and fall back to the snapshot once Kafka has deleted the
segments that held it. A log in a state that Kafka doesn't leave it in is refused, such
as one whose segments start after offset 0 without a snapshot of the offsets before
them.

A process that is killed while appending to the log can leave an incomplete batch at the
end of the last segment, which Kafka truncates on startup. The commands read the log
without it, and `recover` truncates it before appending, listing it in the plan under
`incomplete batch`. A batch that is corrupt, or incomplete anywhere else, is refused,
since Kafka would discard every batch after it.

The commands exit with status 0 on success, and status 1 when they fail or the operator
aborts. Errors that abort an operation are detected before anything is modified.

## `probe`

```text
kraft-last-standing probe [--json] METADATA_DIR [METADATA_DIR ...]
```

Reports the last offset, leader epoch, latest snapshot and voter set of each given copy
of the metadata log, sorted with the most progressed first: by leader epoch, and then by
last offset. It only reads files, and the node ID comes from `meta.properties` in the
parent directory, when there is one.

```shell
$ kraft-last-standing probe /var/lib/kafka/data/__cluster_metadata-0
/var/lib/kafka/data/__cluster_metadata-0
  node ID:      1
  last offset:  1621
  leader epoch: 5
  snapshot:     00000000000000001600-0000000005.checkpoint
  voters:       1, 2, 3
```

`--json` outputs the same information as a JSON array, for gathering results from many
hosts with a script. Fields that the text output describes as unknown or none are
`null`.

```shell
$ kraft-last-standing probe --json /var/lib/kafka/data/__cluster_metadata-0
[
  {
    "directory": "/var/lib/kafka/data/__cluster_metadata-0",
    "node_id": 1,
    "last_offset": 1621,
    "leader_epoch": 5,
    "snapshot": "00000000000000001600-0000000005.checkpoint",
    "voter_ids": [
      1,
      2,
      3
    ]
  }
]
```

## `recover`

```text
kraft-last-standing recover
    --metadata-dir METADATA_DIR
    --backup-dir BACKUP_DIR
    [--node-id NODE_ID]
    [--copy-log-from METADATA_DIR]
    [--yes]
```

Shrinks the voter set down to the controller that owns `--metadata-dir`. The controller
must be stopped. The command reads the latest `VotersRecord`, and appends one
`VotersRecord` per voter to remove, each with one voter fewer than the previous, until
only the kept voter remains.

Before modifying anything, the command prints its plan and asks the operator for
confirmation:

```text
Recovery plan
  metadata directory: /var/lib/kafka/data/__cluster_metadata-0
  backup destination: /var/backups/kafka/__cluster_metadata-0.20261001T144217Z
  last offset:        1621
  leader epoch:       5
  snapshot:           00000000000000001600-0000000005.checkpoint
  current voters:     1, 2, 3
  voter to keep:      1 (directory ID J8aAPcfLQt2bqs1JT_rMgQ)
  voters to remove:   3, 2, in this order
  new log segment:    /var/lib/kafka/data/__cluster_metadata-0/00000000000000001622.log
  offsets to append:  1622 to 1623
Modify the metadata log as described above? [y/N]
```

Run the command as the user that owns the log segments, or as root. Created files get
the mode and owner of the existing segments.

When there already is an empty segment at the first offset to append, which Kafka
creates when rolling a segment, the command writes to it, and the plan marks it as
`(existing, empty)`.

A controller that crashes right after downloading a snapshot from the leader can have a
snapshot that is ahead of its log segments. Kafka then deletes the segments on startup,
and continues the log from the end of the snapshot. The command does the same, and the
plan lists the segments under `segments to delete`. [Snapshots ahead of the log]
describes this state in detail.

[Snapshots ahead of the log]: snapshots.md

Whenever the command deletes or replaces segments, it also deletes
`leader-epoch-checkpoint`, which would otherwise keep the leader epochs of the deleted
segments. Kafka rebuilds it from the log on startup.

### `--metadata-dir METADATA_DIR`

The `__cluster_metadata-0` directory of the controller to recover.

### `--backup-dir BACKUP_DIR`

The directory to back up the metadata directory into, before modifying it. The backup is
a timestamped directory holding a copy of the whole metadata directory. Since the
metadata log can contain credentials, only the owner of the backup can access it. The
backup directory is created if missing, and must not be inside the metadata directory.

### `--node-id NODE_ID`

The ID of the voter to keep. By default, this is the `node.id` in `meta.properties`,
which `kafka-storage.sh format` writes to the parent of the metadata directory. The
command then also verifies that the `directory.id` in `meta.properties` matches the
directory ID registered for the voter, since the controller wouldn't consider itself a
voter after recovery otherwise. When `--node-id` is given, `meta.properties` isn't read,
and the directory ID isn't verified.

### `--copy-log-from METADATA_DIR`

Replace the log segments and snapshots of the metadata directory with copies of the log
segments and latest snapshot in this `__cluster_metadata-0` directory, before appending.
Use this when another node, usually a broker, has a more progressed copy of the log than
any surviving controller. Copy that node's metadata directory to the controller's host
first, and point this option at the copy. Segments that the copied snapshot is [ahead
of][snapshots] are left out, and the plan lists them under `segments to skip`. Other
files in the metadata directory, such as indexes, are kept, and Kafka rebuilds them on
startup.

[snapshots]: snapshots.md#copying-the-log

A broker that truncated its log while the cluster was first starting leaves the voter
set out of its snapshots, because of a bug in Kafka. Once Kafka has deleted the segments
from that time, its copy has no voter set, and the command refuses it. Copy the log of
another node instead.

### `--yes`

Don't ask for confirmation.
