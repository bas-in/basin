#!/usr/bin/env bash
# Django live suite: venv + deps, then suite.py. TAP on stdout; noise on stderr.
set -u
cd "$(dirname "$0")"

if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv 1>&2 || { echo "not ok - django.venv # python3 -m venv failed"; exit 0; }
  .venv/bin/pip install --quiet -r requirements.txt 1>&2 \
    || { echo "not ok - django.install # pip install failed"; exit 0; }
fi
exec .venv/bin/python suite.py
