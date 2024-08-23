#!/bin/bash
# Source Sybase environment setup script
source /opt/sybase/SYBASE.sh

# Start Sybase
/opt/sybase/ASE-16_0/bin/dataserver -d/opt/sybase/data/master.dat &

# Wait for Sybase to start
sleep 30

# Create the database
isql -U sa -P $SYBASE_PASSWORD -Q "CREATE DATABASE your_database;"

# Keep the container running
tail -f /dev/null
