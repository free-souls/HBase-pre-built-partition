package com.panda.hbase.tools;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Log4j
public class CreateWithRegion {

    public static final String COMPREESSTYPE = "compressType";
    public static final String DATABLOCKENCODING = "dataBlockEncoding";
    public static final String BLOOMFILTER = "bloomFilter";


    static Map<String, DataBlockEncoding> dataBlockEncodingTypes = new HashMap<String, DataBlockEncoding>();
    static Map<String, Compression.Algorithm> compressionTypes = new HashMap<String, Compression.Algorithm>();
    static Map<String, BloomType> bloomFilterTypes = new HashMap<String, BloomType>();

    static {
        dataBlockEncodingTypes.put("FAST_DIFF", DataBlockEncoding.FAST_DIFF);
        dataBlockEncodingTypes.put("DIFF", DataBlockEncoding.DIFF);
        dataBlockEncodingTypes.put("PREFIX", DataBlockEncoding.PREFIX);
        dataBlockEncodingTypes.put("NONE", DataBlockEncoding.NONE);
        dataBlockEncodingTypes.put("PREFIX_TREE", DataBlockEncoding.PREFIX_TREE);

        compressionTypes.put("SNAPPY", Compression.Algorithm.SNAPPY);
        compressionTypes.put("GZ", Compression.Algorithm.GZ);
        compressionTypes.put("LZO", Compression.Algorithm.LZO);
        compressionTypes.put("LZ4", Compression.Algorithm.LZ4);
        compressionTypes.put("NONE", Compression.Algorithm.NONE);

        bloomFilterTypes.put("ROWCOL", BloomType.ROWCOL);
        bloomFilterTypes.put("ROW", BloomType.ROW);
        bloomFilterTypes.put("NONE", BloomType.NONE);
    }

    public static void main(String[] args) {

        Admin admin = null;
        String tableName = null;
        String columnFamilyName = null;
        int regionReplicates = 1; // default 1
        try {
            Configuration conf = HBaseConfiguration.create();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length < 3) {
                System.err.println("params size must at lest be 3");
                usage();
                System.exit(0);
            }
            if (otherArgs.length == 4) {
                regionReplicates = Integer.valueOf(otherArgs[3]);
            }
            tableName = otherArgs[0];
            columnFamilyName = otherArgs[1];
            int regionNum = Integer.valueOf(otherArgs[2]);

            String compressType = conf.get(COMPREESSTYPE);
            String dataBlockEncodingType = conf.get(DATABLOCKENCODING);
            String bloomFilterType = conf.get(BLOOMFILTER);
            if (compressType == null || compressType.equals("")) {
                compressType = "NONE";
            }
            if (dataBlockEncodingType == null || dataBlockEncodingType.equals("")) {
                dataBlockEncodingType = "NONE";
            }
            if (bloomFilterType == null || bloomFilterType.equals("")) {
                bloomFilterType = "NONE";
            }

            org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory
                .createConnection(conf);
            admin = connection.getAdmin();
            TableName tableNameObj = TableName.valueOf(tableName);
            if (admin.tableExists(tableNameObj)) {
                log.error("table : " + tableName + " already exist.");
                return;
            }

            log.info("compressType is " + compressType +
                " dataBlockEncodingType is " + dataBlockEncodingType +
                " bloomFilterType is " + bloomFilterType +
                " region replicates are " + regionReplicates);

            HTableDescriptor tableDesc = new HTableDescriptor(tableNameObj);
            tableDesc.setRegionReplication(regionReplicates); // 设置region副本数，提高读可用性

            HColumnDescriptor columnDesc = new HColumnDescriptor(columnFamilyName);
            columnDesc.setCompactionCompressionType(compressionTypes.get(compressType));
            columnDesc.setCompressionType(compressionTypes.get(compressType));
            columnDesc.setDataBlockEncoding(dataBlockEncodingTypes.get(dataBlockEncodingType));
            columnDesc.setBloomFilterType(bloomFilterTypes.get(bloomFilterType));

            tableDesc.addFamily(columnDesc);
            admin.createTable(tableDesc, RegionSplitsUtil.splits(regionNum));
            log.info("created table : " + tableName + " successfully.");
        } catch (Exception e) {
            log.error("failed create table :" + tableName, e);
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void usage() {
        log.info(
            "params : -DcompressType=SNAPPY -DdataBlockEncoding=PREFIX_TREE -DbloomFilter=NONE  " +
                " tableName columnFamilyName regionNum [replicateNum]");

    }
}
