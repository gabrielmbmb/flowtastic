#!/bin/bash

set -x 
set -e

black flowtastic tests --check
isort flowtastic tests --check-only
flake8 flowtastic tests
mypy flowtastic tests
