#!/usr/bin/env bash

exit_code=0

python src/covid-19.py --log-level info --source all --drop true --clone true

echo "$0 finished with code $exit_code."
exit $exit_code
