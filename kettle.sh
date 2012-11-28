#!/bin/bash

export KETTLE_HOME=$(pwd)
echo $KETTLE_HOME


cd ~/Applications/kettle/
nohup ./spoon.sh &

