package com.birdsoaring.utils;


import org.apache.kudu.client.KuduException;


public class ReadKuduTable {

    public static void main(String[] args) {
        // TODO insert table name
        String table = "";
        String host = "localhost";
        KuduUtil utils = new KuduUtil(host);
        try {
            utils.readTablePrint(table);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}