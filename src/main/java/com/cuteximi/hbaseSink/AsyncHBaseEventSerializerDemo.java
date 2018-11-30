package com.cuteximi.hbaseSink;

import com.google.common.base.Charsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @program: weathermrs
 * @description: 同步数据
 * @author: TSL
 * @create: 2018-11-30 22:32
 **/
public class AsyncHBaseEventSerializerDemo implements AsyncHbaseEventSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncHBaseEventSerializerDemo.class);
    private byte[] table;
    private byte[] cf;
    private byte[] payload;
    private byte[] payloadColumn;
    private byte[][] columns;
    private byte[] incrementRow;
    private byte[] incrementColumn;

    public AsyncHBaseEventSerializerDemo() {
    }

    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.cf = cf;
    }

    public List<PutRequest> getActions() {
        List<PutRequest> actions = new ArrayList();
        if (this.columns.length == 0) {
            LOGGER.info("the number of columns is 0");
            return actions;
        } else {
            String[] values = (new String(this.payload)).split(",", -1);
            if (this.columns.length != values.length) {
                LOGGER.info("column name and column value do not match");
                return actions;
            } else {
                byte[] currentRowkey = (values[0] + "&" + values[4]).getBytes();
                byte[][] vs = new byte[this.columns.length][];

                for(int i = 0; i < values.length; ++i) {
                    vs[i] = values[i].getBytes();
                }

                PutRequest put = new PutRequest(this.table, currentRowkey, this.cf, this.columns, vs);
                actions.add(put);
                return actions;
            }
        }
    }

    public List<AtomicIncrementRequest> getIncrements() {
        List<AtomicIncrementRequest> actions = new ArrayList();
        if (this.incrementColumn != null) {
            AtomicIncrementRequest inc = new AtomicIncrementRequest(this.table, this.incrementRow, this.cf, this.incrementColumn);
            actions.add(inc);
        }

        return actions;
    }

    public void cleanUp() {
    }

    public void configure(Context context) {
        String pCol = context.getString("payloadColumn", "pCol");
        String iCol = context.getString("incrementColumn", "iCol");
        if (pCol != null && !pCol.isEmpty()) {
            this.payloadColumn = pCol.getBytes(Charsets.UTF_8);
            String[] columnNames = (new String(this.payloadColumn)).split(",", -1);
            if (columnNames != null && columnNames.length != 0) {
                this.columns = new byte[columnNames.length][];

                for(int i = 0; i < columnNames.length; ++i) {
                    this.columns[i] = columnNames[i].getBytes();
                }
            }
        }

        if (iCol != null && !iCol.isEmpty()) {
            this.incrementColumn = iCol.getBytes(Charsets.UTF_8);
        }

        this.incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
    }

    public void setEvent(Event event) {
        this.payload = event.getBody();
    }

    public void configure(ComponentConfiguration conf) {
    }
}
