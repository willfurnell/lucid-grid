#!/usr/bin/env bash

# Copy over file from QMUL storage
cp /mnt/lustre_2/storm_3/cernatschool.org/data/lucid/$1.tar.gz .
cp /mnt/lustre_2/storm_3/cernatschool.org/data/lucid/db.db .
cp /mnt/lustre_2/storm_3/cernatschool.org/data/lucid/all_tles.json .

# Untar the files we need to work with before we can get started
tar -xzf $1.tar.gz

# Run the analysis! This will be fun :)
/cvmfs/researchinschools.egi.eu/software/miniconda3/bin/python3 analyse.py $1 $2
