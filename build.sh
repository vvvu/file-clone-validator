#!/bin/bash

# Define the desired combinations of GOOS and GOARCH
#PLATFORMS=("windows/amd64" "linux/amd64" "darwin/amd64" "linux/arm" "linux/arm64")

# Not all combinations are valid. If you want to add more platforms, make sure
# the core/metadata/meta_{OS}.go file implements the new OS.
PLATFORMS=("linux/amd64" "darwin/amd64" "linux/arm" "linux/arm64" "linux/386")
# Add more combinations as needed

# Name of the binary
BIN_NAME="validator"

# Output directory
OUTPUT_DIR="./bin"

# Loop through each platform and build
for PLATFORM in "${PLATFORMS[@]}"; do
    GOOS=${PLATFORM%/*}
    GOARCH=${PLATFORM#*/}
    OUTPUT_NAME=$OUTPUT_DIR'/'$BIN_NAME'_'$GOOS'_'$GOARCH
    if [ $GOOS = "windows" ]; then
        OUTPUT_NAME+='.exe'
    fi

    # Setup the environment and build
    env GOOS=$GOOS GOARCH=$GOARCH go build -o $OUTPUT_NAME

    # Check if build was successful
    if [ $? -ne 0 ]; then
        echo 'An error has occurred! Aborting the script execution...'
        exit 1
    fi

    echo 'Building for platform:' $GOOS'/'$GOARCH', Output:' $OUTPUT_NAME
done
