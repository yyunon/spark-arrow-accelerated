#!/bin/bash
docker build --tag arrow:Doc .
docker run --detach -it --name ar arrow:Doc
