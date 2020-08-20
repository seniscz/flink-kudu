package com.birdsoaring.utils;

import org.apache.kudu.client.*;


public class InsertKuduTable {

    public static void main(String[] args) {
        // TODO insert table name
        String tableName = "";
        String host = "localhost";

        long startTime = System.currentTimeMillis();
        KuduClient client = new KuduClient.KuduClientBuilder(host).build();
        insertToKudu(client, tableName);

        long endTime = System.currentTimeMillis();
        //divide by 1000000 to get milliseconds.
        System.out.println("Program executed in " + (endTime - startTime) / 1000 + " seconds");
    }


    private static void insertToKudu(KuduClient client, String tableName) {
        try {
            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            for (int i = 0; i < 1000; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "This is the row number: " + i);
                session.apply(insert);
            }
            session.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
