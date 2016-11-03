package com.abc.carrygo.store.hbase;

import com.abc.carrygo.common.hbase.HBaseConnector;
import com.abc.carrygo.common.zookeeper.ZkAddress;
import com.abc.carrygo.protocol.SQLEventEntry.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by plin on 10/21/16.
 */
public class HBaseWriterTest extends HBaseWriterBase {
    private static final String NS = "example";
    private static final String DB = "test";
    private static final String TABLE = "user";

    private static final String ZK_IP = "127.0.0.1";
    private static final String ZK_PORT = "2181";
    private static final String ZK_DESTINATION = "example";
    private static final String PRIMARY_KEY = "id";
    private static HBaseWriter hbaseWriter;
    private AtomicLong atomicId = new AtomicLong(1);

    @Before
    public void setUp() {
        init();

        ddlBuilder = ddlBuilder.setDb(DB).setTable(TABLE);
        dmlBuilder.setDb(DB).setTable(TABLE);

        ZkAddress zkAddress = new ZkAddress(ZK_DESTINATION, ZK_IP, ZK_PORT);
        try {
            HBaseConnector.init(zkAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hbaseWriter = new HBaseWriter();
    }

    //    @Test
    public void testCreate() {

        DDLCol col = buildDDLCol(PRIMARY_KEY, KeyWord.CREATE, true);
        DDLEntry ddlEntry = buildDDLEntry(KeyWord.CREATE, col);
        SQLEntry sqlEntry = buildSQLEntry(ddlEntry);

        test(sqlEntry);
    }

    //    @Test
    public void testAlter() {
        String colName = "name";

        DDLCol col = buildDDLCol(colName, KeyWord.ADD, false);
        DDLEntry ddlEntry = buildDDLEntry(KeyWord.ALTER, col);
        SQLEntry sqlEntry = buildSQLEntry(ddlEntry);

        test(sqlEntry);
    }

    //    @Test
    public void testInsert() {
        long value = atomicId.addAndGet(System.nanoTime());

        DMLCol col = buildDMLCol(PRIMARY_KEY, String.valueOf(value), true, true);
        DMLEntry dmlEntry = buildDMLEntry(KeyWord.INSERT, col);
        SQLEntry sqlEntry = buildSQLEntry(dmlEntry);

        test(sqlEntry);
    }

    //    @Test
    public void testUpdate() {
        long value = atomicId.get();

        DMLCol col = buildDMLCol(PRIMARY_KEY, String.valueOf(value), true, true);
        DMLEntry dmlEntry = buildDMLEntry(KeyWord.UPDATE, col);
        SQLEntry sqlEntry = buildSQLEntry(dmlEntry);

        test(sqlEntry);
    }

    //    @Test
    public void testDel() {
        long value = atomicId.get();

        DMLCol col = buildDMLCol(PRIMARY_KEY, String.valueOf(value), true, true);
        DMLEntry dmlEntry = buildDMLEntry(KeyWord.DELETE, col);
        SQLEntry sqlEntry = buildSQLEntry(dmlEntry);

        test(sqlEntry);
    }

    //    @Test
    public void testDelTable() {
        HBaseWriter.EventWriter eventWriter = hbaseWriter.getWriter();

        try {
            eventWriter.del(TABLE);
        } catch (Exception e) {
            e.printStackTrace();

            Assert.fail();
        }

    }

    @Test
    public void testDelTables() {
        HBaseWriter.EventWriter eventWriter = hbaseWriter.getWriter();
        List<String> tables = eventWriter.getTables(NS);

        try {
            for (String table : tables) {
                eventWriter.del(table);
            }
        } catch (Exception e) {
            e.printStackTrace();

            Assert.fail();
        }

    }

    private void test(SQLEntry entry) {
        try {
            hbaseWriter.write(entry);
        } catch (Exception e) {
            e.printStackTrace();

            Assert.fail();
        }

        Assert.assertTrue(true);
    }

    @After
    public void tearDown() {
        try {
            hbaseWriter.close();
        } catch (Exception e) {
            e.printStackTrace();

            Assert.fail();
        }
    }
}
