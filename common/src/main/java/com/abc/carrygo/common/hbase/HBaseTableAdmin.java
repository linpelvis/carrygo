package com.abc.carrygo.common.hbase;

import com.abc.carrygo.common.zookeeper.ZkAddress;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class HBaseTableAdmin {
    private static final Logger log = LoggerFactory.getLogger(HBaseTableAdmin.class);

    private Admin admin;

    public HBaseTableAdmin() {
        admin = HBaseConnector.getAdmin();
    }

    public static void main(String[] args) throws Exception {

        Options opts = new Options();
        //-h help
        opts.addOption("h", false, "Print help for this application");
        //-z zookeeper host ips
        opts.addOption("z", true, "The zookeeper host ips, ip1,ip2,ip3");
        //-p zookeeper client port
        opts.addOption("p", true, "The zookeeper client port");
        //-t hbase table name
        opts.addOption("t", true, "The hbase table name");
        //-f column family
        opts.addOption("f", true, "The hbase table column family name");
        //-s start row key
        opts.addOption("s", true, "The start row key");
        //-e end row key
        opts.addOption("e", true, "The end row key");
        //-c region count
        opts.addOption("c", true, "The hbase table region count");
        //-k row key type
        // hex: 16x, format: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
        // uuid: 16x, format: FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF
        opts.addOption("k", true, "The row key type, hex: 16x, forsmat: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF; uuid: 16x, format: FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF;");
        //-a compressin algorithm none snappy lzo etc
        opts.addOption("a", true, "The compressin algorithm");


        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(opts, args);
        if (cl.hasOption('h')) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("OptionsTip", opts);
        }

        String zips = cl.getOptionValue("z");
        String zport = cl.getOptionValue("p");
        String tableName = cl.getOptionValue("t");
        String columnFamily = cl.getOptionValue("f");
        String startKey = cl.getOptionValue("s");
        String endKey = cl.getOptionValue("e");
        String regionCount = cl.getOptionValue("c");
        String rowKeyType = cl.getOptionValue("k");
        String comprAlgorithm = cl.getOptionValue("a");

        if (rowKeyType == null || "".equals(rowKeyType)) {
            log.error("invalid rowKeyType");
            return;
        }

        HBaseConnector.init(new ZkAddress("example", "127.0.0.1", "2181"));

        HBaseTableAdmin hBaseTableAdmin = new HBaseTableAdmin();

//        hBaseTableAdmin.createTable(tableName, columnFamily, comprAlgorithm, startKey, endKey, regionCount, rowKeyType);
        hBaseTableAdmin.createTable(tableName, columnFamily);

        HBaseConnector.close();
    }

    //startKey 16进制数字字符串
    //endKey 16进制数字字符串
    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);

        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));

        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    //startKey 16进制数字字符串 格式,36bits: 00000000-0000-0000-0000-000000000000
    //endKey 16进制数字字符串 格式,36bits: FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF
    public static byte[][] getOdpsUuidSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];

        String[] startKeys = startKey.split("-");
        String[] endKeys = endKey.split("-");

        if (startKeys != null && endKeys != null &&
                startKeys.length == endKeys.length) {
            for (int i = 0; i < startKeys.length; i++) {
                if (startKeys[i].length() != endKeys[i].length()) {
                    log.error("invalid startKey or endKey, corresponding length does not match.");
                    return null;
                }
            }


            BigInteger lowestKey = new BigInteger(startKey.replaceAll("-", ""), 16);
            BigInteger highestKey = new BigInteger(endKey.replaceAll("-", ""), 16);

            BigInteger range = highestKey.subtract(lowestKey);
            BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));

            StringBuffer sbuffer = new StringBuffer();
            lowestKey = lowestKey.add(regionIncrement);
            for (int i = 0; i < numRegions - 1; i++) {
                BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
                String skey = String.format("%016x", key);

                int delta = 0;
                for (int j = 0; j < startKeys.length; j++) {
                    int length = startKeys[j].length();

                    if (j < (startKeys.length - 1)) {
                        String k = skey.substring(delta, delta + length) + "-";
                        sbuffer.append(k);
                    } else if (j == (startKeys.length - 1)) {
                        String k = skey.substring(delta);
                        sbuffer.append(k);
                    }

                    delta += length;
                }


                byte[] b = sbuffer.toString().getBytes();
                splits[i] = b;
                sbuffer.setLength(0);
            }

            return splits;
        } else {
            log.error("invalid startKey or endKey, length does not match.");
        }

        return null;
    }

    public void close() throws Exception {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch (Exception e) {
            log.error("hbase admin close exception occured.", e);
            throw e;
        }
    }

    public boolean exists(String tableName) throws IOException {
        log.info("exists tableName:{}", tableName);
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void add(String tableName, List<String> cols) throws Exception {
        log.info("add col, tableName:{}, cols:{}", tableName, cols);

        if (cols == null || cols.isEmpty()) {
            return;
        }

        TableName tableName1 = TableName.valueOf(tableName);

        try {
            admin.disableTable(tableName1);

            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tableName1);

            HColumnDescriptor[] hColumnDescriptors = hTableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnDescriptor : hColumnDescriptors) {
                cols.remove(columnDescriptor.getNameAsString());
            }

            if (!cols.isEmpty()) {
                for (String col : cols) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(col));
                }
            }

            admin.modifyTable(tableName1, hTableDescriptor);
        } catch (Exception e) {
            log.error("hbase addCols failed. tableName:{}, cols:{}", tableName1, cols, e);
            throw e;
        } finally {
            admin.enableTable(tableName1);
        }
    }

    public void del(String tableName, List<String> cols) throws IOException {
        log.info("del col, tableName:{}, cols:{}", tableName, cols);

        if (cols == null || cols.isEmpty()) {
            return;
        }

        TableName tableName1 = TableName.valueOf(tableName);

        try {
            admin.disableTable(tableName1);

            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tableName1);
            for (String col : cols) {
                hTableDescriptor.removeFamily(col.getBytes());
            }

            admin.modifyTable(tableName1, hTableDescriptor);
        } catch (Exception e) {
            log.error("hbase deleteCols failed. tableName:{}, cols:{}", tableName1, cols, e);
            throw e;
        } finally {
            admin.enableTable(tableName1);
        }
    }

    public void delTable(String tableName) throws IOException {
        log.info("delTable tableName:{}", tableName);

        TableName tableName1 = TableName.valueOf(tableName);

        try {
            admin.disableTable(tableName1);

            admin.deleteTable(tableName1);
        } catch (Exception e) {
            log.error("hbase del table failed. tableName:{}", tableName1, e);
            throw e;
        }
    }

    public List getTables(String namespace) throws IOException {
        log.info("getTables namespace:{}", namespace);

        List tables = new ArrayList();

        try {
            TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
            for (TableName name : tableNames) {
                tables.add(name.getNameAsString());
            }
        } catch (Exception e) {
            log.error("hbase get tables failed. namespace:{}", namespace);
            throw e;
        }

        return tables;
    }

    public void createNamespace(String name) throws IOException {
        log.info("createNamespace namespace:{}", name);

        admin.createNamespace(NamespaceDescriptor.create(name).build());
    }

    public boolean hasNamespace(String name) {
        String ns = null;
        try {
            ns = admin.getNamespaceDescriptor(name).getName();
        } catch (IOException e) {
            log.warn("hbase namespace is not exist, name:{}.", name);
        }

        return StringUtils.equalsIgnoreCase(ns, name);
    }

    public void createTable(
            String tableName,
            String columnFamily) throws IOException {
        log.info("hbase create table, table:{}, colf:{}", tableName, columnFamily);

        createTable(tableName, columnFamily, "", "", "", "", "");
    }

    public void createTable(
            String tableName,
            String columnFamily,
            String comprAlgorithm,
            String startKey,
            String endKey,
            String regionCount,
            String rowKeyType) throws IOException {

        TableName tableNmae = TableName.valueOf(tableName);
        if (admin.tableExists(tableNmae)) {
            log.error("The table " + tableName + " already exists.");
        } else {
            HTableDescriptor tableDescp = new HTableDescriptor(tableNmae);

            getHColDescriptors(columnFamily, comprAlgorithm, tableDescp);

            byte[][] splitKeys = null;

            if (!StringUtils.isAnyEmpty(startKey, endKey, regionCount, rowKeyType)) {
                int regions = Integer.parseInt(regionCount);
                if (rowKeyType != null && "uuid".equalsIgnoreCase(rowKeyType)) {
                    splitKeys = getOdpsUuidSplits(startKey, endKey, regions);
                } else {
                    splitKeys = getHexSplits(startKey, endKey, regions);
                }
            }

            if (splitKeys != null && splitKeys.length > 0) {
                admin.createTable(tableDescp, splitKeys);
            } else {
                log.warn("hbase table created with no pre-splits.");
                admin.createTable(tableDescp);
            }
        }
    }

    private void getHColDescriptors(String columnFamily, String comprAlgorithm, HTableDescriptor tableDescp) {
        HColumnDescriptor colFamilyDescp = new HColumnDescriptor(columnFamily);
        if ("snappy".equalsIgnoreCase(comprAlgorithm)) {
            colFamilyDescp.setCompressionType(Algorithm.SNAPPY);
        } else if ("gz".equalsIgnoreCase(comprAlgorithm)) {
            colFamilyDescp.setCompressionType(Algorithm.GZ);
        } else if ("lz4".equalsIgnoreCase(comprAlgorithm)) {
            colFamilyDescp.setCompressionType(Algorithm.LZ4);
        } else if ("lzo".equalsIgnoreCase(comprAlgorithm)) {
            colFamilyDescp.setCompressionType(Algorithm.LZO);
        } else {
            colFamilyDescp.setCompressionType(Algorithm.NONE);
        }
        tableDescp.addFamily(colFamilyDescp);
    }

}
