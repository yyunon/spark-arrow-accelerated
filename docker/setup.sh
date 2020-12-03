#!/bin/bash

export WORK=/home/yyonsel/bulk/project
echo "Setting up WORK env as ... $WORK"
export  FLETCHER_DIR=/home/yyonsel/bulk/fletcher

echo "Setting up PATH and LD_LIB"
export PATH=$WORK/local/bin:$PATH
export LD_LIBRARY_PATH=$WORK/local/lib:$WORK/local/lib64:$PATH

