# syntax=docker/dockerfile:1
FROM ubuntu:20.04
WORKDIR /ACE
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y build-essential libssl-dev libffi-dev python3-dev python3-pip libsasl2-dev libldap2-dev default-libmysqlclient-dev
RUN apt-get install -y libmariadb-dev
RUN git clone https://github.com/totemtang/ACE.git
