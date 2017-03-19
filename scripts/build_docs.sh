#!/usr/bin/env bash

pdoc \
		--html \
		--overwrite \
		--template-dir=scripts/templates/ \
		--html-dir=docs/ \
		cupboard &&
	cd docs && \
	mv cupboard/* . && \
	rm -fr cupboard/