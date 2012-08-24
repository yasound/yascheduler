#!/bin/bash
# This script bootstraps a virtualenv 
# environement for demos

# Starts
if [ ! -f ./vtenv/bin/python ]
then
    if [ ! -f ./virtualenv.py ]
    then
        echo "Please get a copy of virtualenv.py"
        exit 1
    fi
    python virtualenv.py vtenv
fi
vtenv/bin/pip install -r requirements.txt --download-cache=eggs

if [ ! -d ./vtenv/src/ssh ];then
    vtenv/bin/pip install -e git://github.com/bitprophet/ssh.git#egg=ssh || echo "Please install python-dev and retry"
else
    cd vtenv/src/ssh
    git pull origin master
    cd ../../..
fi

if [ ! -d ./vtenv/src/fabric ];then
    vtenv/bin/pip install -e git://github.com/fabric/fabric.git#egg=fabric
else
    cd vtenv/src/fabric
    git pull origin master
    cd ../../..
fi
if [ ! -f ./fab ];then
    echo " * Now you can type ./fab prepare to init your dir"
    ln -s vtenv/bin/fab fab
fi

echo " * Done"
exit

