#!/bin/bash
# Install system dependencies required for PyArrow
apt-get update && apt-get install -y \
    cmake \
    libboost-all-dev \
    libssl-dev

# Install Python packages with wheel preference
pip install --prefer-binary -r requirements.txt
