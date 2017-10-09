if [ -z "$CONFIG" ]; then
if [ -f "config" ]
then
    CONFIG=`pwd`/config
else
   CONFIG=`dirname $0`/config
fi
fi
echo "use config file:", $CONFIG
. $CONFIG

