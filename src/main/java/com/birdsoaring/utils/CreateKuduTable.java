package com.birdsoaring.utils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chezhao
 */
public class CreateKuduTable {
    private static String kuduMaster = "dev-kudu01,dev-kudu02,dev-kudu03";

    public static void main(String[] args) {
        CreateKuduTable createKuduTable = new CreateKuduTable();
        //createKuduTable.creatTable();
        KuduUtil kuduUtil = new KuduUtil(kuduMaster);
        RowSerializable rowSerializable = new RowSerializable(2);
        String[] columns = {"1","cz"};
        String tableName = "test";
        KuduTable kuduTable = kuduUtil.useTable(tableName);
        kuduUtil.insert(kuduTable,rowSerializable,columns);
    }

    public void creatTable() {
        String tableName = "test";
        String host = kuduMaster;
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(host).build();
        try {
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("id");
            Schema schema = new Schema(columns);
            kuduClient.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4));
            System.out.println("Table \"" + tableName + "\" created succesfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}




