#!/bin/sh

SERVER_LIST="carry writer"

CARRY_MAIN_CLASS="com.abc.carrygo.parser.server.CarryServer"
WRITER_MAIN_CLASS="com.abc.carrygo.store.server.WriterServer"


JAVA_OPTS="-server -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError -Xloggc:gc.log -XX:+UseConcMarkSweepGC"
JAVA_OPTS="$JAVA_OPTS -XX:MaxPermSize=512m"
