package com.birdsoaring.sink;

import com.birdsoaring.utils.KuduUtil;
import com.birdsoaring.utils.RowSerializable;
import com.birdsoaring.utils.exceptions.KuduClientException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KuduSink extends RichSinkFunction<RowSerializable> {
    private final static Logger logger = LoggerFactory.getLogger(KuduSink.class);
    private String host, tableName;
    private String[] fieldsNames;
    private transient KuduUtil utils;

    //Kudu variables
    private transient KuduTable table;


    /**
     * Builder to use when you want to create a new table
     *
     * @param host        Kudu host
     * @param tableName   Kudu table name
     * @param fieldsNames List of column names in the table to be created
     * @throws KuduClientException In case of exception caused by Kudu Client
     */
    public KuduSink(String host, String tableName, String[] fieldsNames) throws KuduClientException {

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty() || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");

        }
        this.host = host;
        this.tableName = tableName;
        this.fieldsNames = fieldsNames;
    }

    /**
     * Builder to be used when using an existing table
     *
     * @param host      Kudu host
     * @param tableName Kudu table name
     * @throws KuduClientException In case of exception caused by Kudu Client
     */
    public KuduSink(String host, String tableName) throws KuduClientException {

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");
        }
        this.host = host;
        this.tableName = tableName;
    }

    /**
     * It's responsible to insert a row into the indicated table by the builder (Streaming)
     *
     * @param row Data of a row to insert
     */
    @Override
    public void invoke(RowSerializable row) throws IOException {
        // Establish connection with Kudu
        if (this.utils == null) {
            this.utils = new KuduUtil(host);
        }
        if (this.table == null) {
            this.table = this.utils.useTable(tableName, fieldsNames, row);
        }
        // Make the insert into the table
        utils.insert(table, row, fieldsNames);
        logger.info("Inserted the Row: | " + utils.printRow(row) + "at the table \"" + this.tableName + "\"");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}