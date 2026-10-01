SHELL := /usr/bin/env bash

.PHONY: test
test:
	pytest $(test)

.PHONY: test-integration
test-integration:
	pytest -m integration $(test)

.PHONY: docs
docs:
	mkdocs build --strict

.PHONY: docs-serve
docs-serve:
	mkdocs serve --strict

.PHONY: clean
clean:
	rm -rf {**/,}*.egg-info **{/**,}/__pycache__ build dist site .coverage coverage.xml

.PHONY: requirements
requirements: typing-requirements docs-requirements

.PHONY: typing-requirements
typing-requirements: export UV_CUSTOM_COMPILE_COMMAND='make typing-requirements'
typing-requirements:
	@uv pip compile \
		--generate-hashes \
		--strip-extras \
		--extra=type-check \
		--upgrade \
		--universal \
		--python-version=3.12 \
		--output-file=typing-requirements.txt \
		pyproject.toml

.PHONY: docs-requirements
docs-requirements: export UV_CUSTOM_COMPILE_COMMAND='make docs-requirements'
docs-requirements:
	@uv pip compile \
		--generate-hashes \
		--strip-extras \
		--extra=docs \
		--upgrade \
		--universal \
		--python-version=3.12 \
		--output-file=docs-requirements.txt \
		pyproject.toml
