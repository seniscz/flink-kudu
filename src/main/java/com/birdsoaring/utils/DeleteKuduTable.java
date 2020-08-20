package com.birdsoaring.utils;

import org.apache.kudu.client.KuduClient;


public class DeleteKuduTable {
    public static void main(String[] args) {
        String tableName = "";
        String host = "localhost";
        KuduClient client = new KuduClient.KuduClientBuilder(host).build();
        try {
            client.deleteTable(tableName);
            System.out.println("Table \"" + tableName + "\" deleted succesfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
