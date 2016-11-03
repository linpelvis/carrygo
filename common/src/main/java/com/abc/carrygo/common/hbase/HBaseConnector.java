package com.abc.carrygo.common.hbase;

import com.abc.carrygo.common.zookeeper.ZkAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by plin on 10/13/16.
 */
public class HBaseConnector {
    private static final Logger log = LoggerFactory.getLogger(HBaseConnector.class);

    private static Connection connection;

    private static Admin admin;

    public static void init(ZkAddress zkAddress) throws IOException {
        //configuration.set("hbase.zookeeper.quorum","10.10.3.181,10.10.3.182,10.10.3.183");
        //configuration.set("hbase.zookeeper.property.clientPort","2181");
        System.setProperty("hadoop.home.dir", "/");

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkAddress.getZkIp());
        configuration.set("hbase.zookeeper.property.clientPort", zkAddress.getZkPort());
        configuration.set("zookeeper.znode.parent", "/hbase");

        configuration.set("hbase.client.retries.number", "3");  // default 35
        configuration.set("hbase.rpc.timeout", "10000");  // default 60 secs
        configuration.set("hbase.rpc.shortoperation.timeout", "5000"); // default 10 secs


//        if (confs != null && confs.size() > 0) {
//            for (String key : confs.keySet()) {
//                configuration.set(key, confs.get(key));
//            }
//        }

        try {
            //hbase-client中protobuf版本与hbase不兼容会导致无法连接的问题
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (Exception e) {
            log.error("hbase init exception occured.", e);
            throw e;
        }
    }

    public static void close() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            log.error("hbase connection close exception occured.", e);
            throw e;
        }
    }

    public static Admin getAdmin() {
        return admin;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static Table getTable(String tableName) throws Exception {
        Table table = null;
        try {
            if (connection != null) {
                table = connection.getTable(TableName.valueOf(tableName));
            }
        } catch (Exception e) {
            log.error("get table " + tableName + " failed.", e);
            throw e;
        }
        return table;
    }
}

