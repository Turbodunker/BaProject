#! /bin/bash
# A script to run all tests. This will automatically move to the tests 
# directory and call all other files as pytest scripts

# Need to more to local dir to run tests
starting_working_dir=$(pwd)
script_name=$(basename "$0")
script_dir=$(dirname "$(realpath "$0")")

skip_long_tests=0

for arg in "$@"
do
  if [[ $arg == -s ]]
  then
    skip_long_tests=1
  fi
done

cd $script_dir

# Gather all other test files and run pytest
search_dir=.
for entry in "$search_dir"/*
do
  if  [[ $entry == ./test* ]] \
    && [[ -f $entry ]] \
    && [[ $entry != ./$script_name ]] \
    && [[ $entry != ./shared.py ]];
  then
    SKIP_LONG=$skip_long_tests python3 "-m" pytest $entry "-W ignore::DeprecationWarning"
  fi
done

# Move back to where we called from
cd $starting_working_dir
