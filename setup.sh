#!/bin/bash

# Define directory names
DIR=$(pwd)
DATA="$DIR/data"

# Define file names
REFERENCES="references"
CORINE="clc18.7z"

# Define remote file paths
CORINE_RPATH="ftp://Corine_Land_Cover_ext:ishiteimapie9ahP@ftp3.ign.fr/CLC18_SHP__FRA_2019-08-21.7z" 

# Create directories
mkdir -p "$DATA"

# Download data
cd "$DATA"

if [ ! -f "$CORINE" ]
then
    echo "Downloading $CORINE...\n"
    wget "$CORINE_RPATH" -O "$CORINE" --limit-rate=300k
fi
