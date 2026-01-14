#!/bin/bash

if hadoop fs -rm -r -f /user/vagrant/project/WDI
then
    echo "---> WDI directory deleted."
else
    echo "---> ERROR: Deleting WDI directory failed."
    exit 1
fi

if hadoop fs -mkdir -p /user/vagrant/project/WDI
then
    echo "---> WDI directory created successfully."
else
    echo "---> ERROR: Creating WDI directory failed."
    exit 1
fi
