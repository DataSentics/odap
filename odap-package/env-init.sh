#!/bin/bash

# Activate conda
eval "$(conda shell.bash hook)"

# Create new conda environment and activate it
conda env create -f environment.yml -p .venv
conda activate $PWD/.venv

# Install all dependencies
poetry install

# Copy dotenv
cp .env.dist .env
