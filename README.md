<h1 align=center>kraft-last-standing</h1>

<p align=center>
    <a href="https://github.com/Aiven-Open/kraft-last-standing/actions/workflows/ci.yaml?query=branch%3Amain"><img src="https://github.com/Aiven-Open/kraft-last-standing/actions/workflows/ci.yaml/badge.svg?branch=main" alt="CI Build Status"></a>
    <br>
    <a href=https://pypi.org/project/kraft-last-standing/><img src=https://img.shields.io/pypi/v/kraft-last-standing.svg?color=informational&label=PyPI alt="PyPI Package"></a>
    <a href=https://pypi.org/project/kraft-last-standing/><img src=https://img.shields.io/pypi/pyversions/kraft-last-standing.svg?color=informational&label=Python alt="Python versions"></a>
</p>

<p align=center>
    Recover an Apache Kafka® cluster that has permanently lost its KRaft controller quorum.
</p>

<h4 align=center>
    <a href=https://aiven-open.github.io/kraft-last-standing/>Checkout the complete documentation →</a>
</h4>

When a dynamic KRaft quorum loses a majority of its voters, the lost voters can only be
removed with quorum, and quorum can only be restored by removing the lost voters.
kraft-last-standing breaks that loop by appending `VotersRecord` control records to the
metadata log of a stopped, surviving controller, shrinking the voter set down to only
that controller. Started again, it elects itself leader, and the cluster can make
progress.

<!-- prettier-ignore-start -->
> [!CAUTION]
> Recovery cements data loss. Anything that only existed on the lost nodes is gone, and
> this tool cannot bring it back. Only use it once it is verified that the lost voters
> cannot be recovered, and follow the [runbook].
<!-- prettier-ignore-end -->

[runbook]: https://aiven-open.github.io/kraft-last-standing/latest/runbook/

The blog post [Never lose quorum in Apache Kafka®][blog] explains the approach and its
consequences.

[blog]: https://aiven.io/blog/kafka-never-lose-quorum

## Installation

```shell
$ pip install --require-virtualenv kraft-last-standing
```

## Usage

Find the most progressed copy of the metadata log, by probing every surviving broker and
controller.

```shell
$ kraft-last-standing probe /var/lib/kafka/data/__cluster_metadata-0
```

Then, with every controller stopped, recover the chosen one.

```shell
$ kraft-last-standing recover \
    --metadata-dir /var/lib/kafka/data/__cluster_metadata-0 \
    --backup-dir /var/backups/kafka
```

## Development

Install development requirements.

```shell
$ pip install --require-virtualenv -e '.[type-check,docs]'
```

Run unit tests, type check, and build the documentation.

```shell
$ make test
$ mypy
$ make docs
```

The integration tests run Apache Kafka® controllers in containers, using the
`apache/kafka` image, and require Docker. The `KAFKA_VERSION` environment variable
selects the version of the image, and defaults to 4.3.1.

```shell
$ make test-integration
```

The tests bind mount their temporary directories into the containers. When Docker runs
in a virtual machine that only shares the home directory, which is the default for
Colima, point pytest's temporary directory there.

```shell
$ mkdir -p ~/.cache/kraft-last-standing
$ make test-integration test=--basetemp=$HOME/.cache/kraft-last-standing/pytest
```

Lint and format with [goose].

```shell
$ goose run --select=all
```

[goose]: https://github.com/antonagestam/goose
