package com.panda.hbase.tools;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class GenRowkeyUtilTest {

    @Test
    public void genKey() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.1.5.15,10.1.5.17,10.1.5.19");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase");

        HBaseAdmin admin = new HBaseAdmin(conf);
        String tableName = "test1";
        String familyName = "cf";
        String columnName = "imageColumn";
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hdc = new HColumnDescriptor(familyName);
        htd.addFamily(hdc);

        HTable table = new HTable(conf, htd.getName());
        table.setAutoFlush(false);

        long begin = System.currentTimeMillis();
        for (int i = 1001; i < 2001; i++) {
            byte[] kkk = new byte[10000 + i / 1000];
            Put p1 = new Put(Bytes.toBytes(GenRowkeyUtil.genKey(i + "")));
            p1.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), kkk);
            table.put(p1);
        }
        long end = System.currentTimeMillis();
        table.flushCommits();
        System.out.println("HBase-use-time:" + (end - begin));
    }

}