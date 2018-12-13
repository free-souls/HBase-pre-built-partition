#!/usr/bin/env bash
#   Automatic generate table of next month
#   You can change 3 argument to control the way of generate table you need.
#	1.table_prefix
#	2.columnName
#	3.regionNum
#
#   For example: If you execute this script, this script will use system date
# to generate table in pattern like <[table_prefix][yyyyMMdd]> from the
# beginning to the end of next month.eg. ./generateTable.sh dr_query f 10
#
#########


tableName=$1
columnName=$2
regionNum=$3

java -jar target/hbase_table_tool-*.jar -DcompressType=SNAPPY \
-DdataBlockEncoding=FAST_DIFF \
-Dhbase.zookeeper.quorum=10.1.5.15,10.1.5.17,10.1.5.19 \
-DbloomFilter=NONE -Dhbase.zookeeper.property.clientPort=2181 \
-Dzookeeper.znode.parent=/hbase \
$tableName $columnName $regionNum