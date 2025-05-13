#!/bin/bash

# --- Controllo input ---
if [ $# -ne 2 ]; then
    echo "ERROR: correct use $0 <START_RUN> <END_RUN>"
    exit 1
fi

START=$1
END=$2
N_JOBS=8

echo "* STEP 1: Process ROOT files in parallel (run $START to $END)"
parallel -j $N_JOBS python3 scripts/process_one.py ::: $(seq $START $END)

echo "* STEP 2: Filter runs in parallel"
parallel  -j $N_JOBS python3 scripts/filter_run.py ::: $(seq $START $END)

echo "* STEP 3: Merge filtered parquet files"
python3 scripts/merge_all_fast.py

echo "* ALL DONE: Pipeline completed for runs $START a $END **"

echo "*  Removing temporary files ..."
rm -r output
rm -r output_filtered
echo "*  Removing temporary files ... done"
