#!/bin/bash
docker rm -f ar &&\
docker build --tag arrow:Doc . &&\
docker run --detach  \
           -it \
           -v /home/yyunon/thesis_journals/resources/spark-arrow-accelerated:/app \
           --name ar arrow:Doc
