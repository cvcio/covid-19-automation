#!/usr/bin/env bash

exit_code=0

if [[ ! -v MONGO_URL ]]; then
    MONGO_URL="mongodb://localhost:27017/"
fi
python src/covid-19.py --log-level debug --source all --drop true --clone true --mongo $MONGO_URL --govgr_token $GOVGR_TOKEN

echo "$0 finished with code $exit_code."
exit $exit_code
