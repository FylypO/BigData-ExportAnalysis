#!/bin/bash

PROJECT_DIR="/user/vagrant/project"

if hadoop fs -test -d "$PROJECT_DIR"; then
    echo "---> Directory $PROJECT_DIR exists. Deleting..."
    
    # Próba usunięcia tylko jeśli istnieje
    if hadoop fs -rm -r -f "$PROJECT_DIR"; then
        echo "---> Project directory deleted successfully."
    else
        echo "---> ERROR: Deleting project structure failed."
        exit 1
    fi
else
    echo "---> Directory $PROJECT_DIR does not exist. Skipping deletion."
fi

if hadoop fs -mkdir -p \
    "$PROJECT_DIR/final_tables" \
    "$PROJECT_DIR/NBP" \
    "$PROJECT_DIR/WDI" \
    "$PROJECT_DIR/comtrade" \
    "$PROJECT_DIR/dim"
then
    echo "---> Project directory structure created successfully."
else
    echo "---> ERROR: Creating project directory structure failed."
    exit 1
fi
