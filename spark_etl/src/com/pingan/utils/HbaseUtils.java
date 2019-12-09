package com.pingan.utils;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.glassfish.grizzly.Connection;

import java.io.IOException;

/**
 * @author liushujie
 * 2018/9/9 10:31
 */
public class HbaseUtils {

    HBaseAdmin admin = null;
    Configuration configration = null;
    private HbaseUtils(){

        configration = new Configuration();
        configration.set("hbase.zookeeper.quorum", "hadoop:2181");
        configration.set("hbase.rootdir", "hdfs://hadoop/hbase");
        try {
            admin = new HBaseAdmin(configration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtils instance = null;

    public static synchronized HbaseUtils getInstance() {
        if (null == instance) {
            instance = new HbaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取htable实例
     * @param tableName
     * @return
     */
    public  HTable getHtable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;

    }

    /**
     * 添加数据到hbase里面
     * @param tableName 表名
     * @param rowKey 对应key的值
     * @param cf    hbase列簇
     * @param colum hbase对应的列
     * @param value hbase对应的值
     */
    public  void put(String tableName,String rowKey,String cf,String colum,String value){
        HTable table = getHtable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(colum),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String tableName = "course_search_clickcount";
        String rowkey="20180909_1";
        String cf = "info";
        String colum = "cagegory_click_count";
        String value = "100";

        HbaseUtils.getInstance().put(tableName,rowkey,cf,colum,value);
    }
}
