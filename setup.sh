#!/bin/bash
# Install system dependencies required for PyArrow
apt-get update && apt-get install -y \
    cmake \
    libboost-all-dev \
    libssl-dev