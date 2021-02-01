#!/bin/bash
sudo docker rm -f ar &&\
sudo docker build --tag arrow:Doc . &&\
sudo docker run --detach  \
           -it \
           -v /home/yyunon/thesis_journals/resources/spark-arrow-accelerated:/app \
           --name ar arrow:Doc
