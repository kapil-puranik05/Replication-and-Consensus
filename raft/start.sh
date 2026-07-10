#!/bin/bash

gnome-terminal -- bash -c '
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8080 --raft.data-dir=configs/node1";
exec bash
'

gnome-terminal -- bash -c '
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8081 --raft.data-dir=configs/node2";
exec bash
'

gnome-terminal -- bash -c '
mvn spring-boot:run "-Dspring-boot.run.arguments=--server.port=8082 --raft.data-dir=configs/node3";
exec bash
'
