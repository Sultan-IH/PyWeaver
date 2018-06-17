#!/usr/bin/env bash

#!/usr/bin/env bash

# stuff that determines the behavior of the program
cat ./pyweaver.env | while read line; do
    export $line
done

# exporting db env variables
cat ./pgdb.env | while read line; do
    export $line
done

# stuff specific to this machine
export NODE_NAME="SULTAN_MAC_TEST"
export PRODUCTION="FALSE"