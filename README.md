# Introduce

该工具用于创建hbase表并预建分区


## Install Stage

1. git clone https://github.com/free-souls/HBase-pre-built-partition.git

2. cd hbase_table_tool

3. mvn clean package

4. sh start.sh <tableName> <column_family> <partionsNums>


### Running hbase_table_tool

Here is the usage:
```
usage: sh start.sh  <tableName> <column_family> <partionsNums> <br/>

    *  注意start.sh中的脚本参数需要根据自己服务器的部署情况做更改,例如下面的情况
    *  -Dhbase.zookeeper.quorum 要指定自己服务器zk的地址
    *  -Dzookeeper.znode.parent znode的路径
    *  -Dhbase.zookeeper.property.clientPor 默认是2181端口 不是需要更改
    *  -DcompressType 指定压缩方式 有<SNAPPY,GZ,LZO,LZ4,NONE>
    *  --DbloomFilter 设置bloomFilter类型 不设置默认是NONE,有<ROWCOL,ROW,NONE>三种
    *  --DdataBlockEncoding 设置DataBlockEncoding压缩算法 不设置默认是NONE,有<FAST_DIFF,PREFIX,NONE,PREFIX_TREE>
    *  <tableName> 要创建的表名 <column_family> 列族 <partionsNums> 要分区的数目
```

