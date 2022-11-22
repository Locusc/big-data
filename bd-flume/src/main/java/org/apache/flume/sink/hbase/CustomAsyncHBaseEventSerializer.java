//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flume.sink.hbase;

import com.google.common.base.Charsets;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jay
 * 自定义flume的hbase序列化器
 * 2022/11/21
 */
public class CustomAsyncHBaseEventSerializer implements AsyncHbaseEventSerializer {

    /**
     * org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType
     */
    public enum KeyType {
        UUID,
        RANDOM,
        TS,
        TSNANO;
    }

    private static final Logger logger = LoggerFactory.getLogger(CustomAsyncHBaseEventSerializer.class);
    private byte[] table;
    private byte[] cf;
    private byte[] payload;
    private byte[] payloadColumn;
    private byte[] incrementColumn;
    private String rowPrefix;
    private byte[] incrementRow;
    private KeyType keyType;

    public CustomAsyncHBaseEventSerializer() {
    }

    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.cf = cf;
    }

    public List<PutRequest> getActions() {
        List<PutRequest> actions = new ArrayList();
        if (this.payloadColumn != null) {
            try {
                logger.info("entry DjtAsyncHbaseEventSerializer");
                logger.info("payloadColumn=" + new String(this.payload));
                String[] columns = (new String(this.payloadColumn)).split(",");
                logger.info("columns=" + new String(this.payloadColumn));
                String[] values = (new String(this.payload)).split(",");
                logger.info("columns size=" + columns.length);
                logger.info("values size=" + values.length);
                if (columns.length != values.length) {
                    return actions;
                }

                String datetime = values[0].toString();
                String userid = values[1].toString();
                byte[] rowKey = SimpleRowKeyGenerator.getDjtRowKey(userid, datetime);
                logger.info("rowKey=" + rowKey);

                for (int i = 0; i < columns.length; ++i) {
                    byte[] colColumn = columns[i].getBytes();
                    byte[] colValue = values[i].getBytes(Charsets.UTF_8);
                    PutRequest putRequest = new PutRequest(this.table, rowKey, this.cf, colColumn, colValue);
                    actions.add(putRequest);
                }

                logger.info("end task!");
            } catch (Exception var11) {
                throw new FlumeException("Could not get row key!", var11);
            }
        }

        return actions;
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
        this.rowPrefix = context.getString("rowPrefix", "default");
        String suffix = context.getString("suffix", "uuid");
        if (pCol != null && !pCol.isEmpty()) {
            if (suffix.equals("timestamp")) {
                this.keyType = KeyType.TS;
            } else if (suffix.equals("random")) {
                this.keyType = KeyType.RANDOM;
            } else if (suffix.equals("nano")) {
                this.keyType = KeyType.TSNANO;
            } else {
                this.keyType = KeyType.UUID;
            }

            this.payloadColumn = pCol.getBytes(Charsets.UTF_8);
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
