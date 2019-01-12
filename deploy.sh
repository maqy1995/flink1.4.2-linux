#!/usr/bin/env bash

rm -r ~/flink1.4.2-maqy

cd ~/flink1.4.2-linux

cp -r ./build-target/ /home/hadoop/flink1.4.2-maqy/

scp -r /home/hadoop/flink1.4.2-maqy/ hadoop@slave1:/home/hadoop/

scp -r /home/hadoop/flink1.4.2-maqy/ hadoop@slave2:/home/hadoop/

scp -r /home/hadoop/flink1.4.2-maqy/ hadoop@slave3:/home/hadoop/
