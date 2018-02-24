package com.datatorrent.example.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.example.ads.AdInfo;
import com.datatorrent.example.dedupapp.RocksDbStore;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class AggregationOperator extends BaseOperator implements Operator.CheckpointNotificationListener{
    private RocksDbStore DBstore = new RocksDbStore();
    long operatorId;
    private transient RocksDB db;
    private final static byte[] EMPTY_VAL = new byte[]{0};
    private static transient Logger logger = LoggerFactory.getLogger(AggregationOperator.class);

    public byte[] getKeyPublisher(Object o)
    {
        AdInfo tuple = (AdInfo)o;

        long millis = tuple.getTimeStamp();
        long minute = (millis / (1000 * 60)) % 60;
        long hour = (millis / (1000 * 60 * 60)) % 24;
        String publisher = tuple.getPublisher();
        String key = hour+":"+minute+""+publisher;
        return key.getBytes();
    }

    public byte[] getKeyAdvertiser(Object o)
    {
       AdInfo tuple = (AdInfo)o;
        long millis = tuple.getTimeStamp();
        long minute = (millis / (1000 * 60)) % 60;
        long hour = (millis / (1000 * 60 * 60)) % 24;
        String advertiser = tuple.getAdvertiser();
        String key = hour+":"+minute+""+advertiser;
        return key.getBytes();
    }

    public byte[] makeValue(Object o)
    {
        AdInfo tuple = (AdInfo)o;
        int count=1;
        String value =String.format("%03d",count)+"#"+String.format("%1$,.2f", tuple.getEcpm());
        return value.getBytes();

    }

    public void updateValue(Object o,byte[] key) throws RocksDBException {
        AdInfo tuple = (AdInfo)o;
        byte[] val = db.get(key);
        String value = new String(val);
        String[] parts = value.split("#");
        int count  = Integer.parseInt(parts[0]);
        count++;
        double newPrice = tuple.getEcpm() + Double.parseDouble(parts[1]);
        String newValue =String.format("%03d",count)+"#"+String.format("%1$,.2f",newPrice);
        db.put(key,newValue.getBytes());
        String result = "key :    "+ new String(key)+"    New Value:   "+newValue;
        outputData.emit(result);
    }

    public transient final DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        public void process(Object o)
        {
            try {
                byte[] keyPublisher = getKeyPublisher(o);
                if(db.get(keyPublisher)==null) {
                    db.put(keyPublisher, makeValue(o));
                }else{
                    updateValue(o,keyPublisher);
                }

                byte[] keyAdvertiser = getKeyAdvertiser(o);
                if(db.get(keyAdvertiser)==null) {
                    db.put(keyAdvertiser,makeValue(o));
                }else{
                    updateValue(o,keyAdvertiser);
                }
            } catch (RocksDBException e) {
                e.printStackTrace();
            }

        }
    };
    public final transient DefaultOutputPort<String> outputData = new DefaultOutputPort<>();

    @Override
    public void setup(Context.OperatorContext context)
    {
        super.setup(context);
        DBstore.setHdfspath("/tmp/rocksbackup/");
       db = DBstore.setDBandFetch(context);

        if (db == null) {
            throw new RuntimeException("Unable to initialize db ");
        }
        operatorId = context.getId();

    }

    @Override
    public void teardown()
    {
        super.teardown();
        DBstore.closeDB();
    }
    @Override
    public void beforeCheckpoint(long windowId)
    {
        logger.info("Before Checkpoint {} windowId = {}", db.getSnapshot(), windowId);
        try {
            DBstore.zipAndSend(operatorId, windowId); //zipping db and pushing into hdfs
        } catch (IOException e) {
            logger.error("Error while taking checkpoint {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void committed(long windowId)
    {
        try {
            DBstore.deleteOlderCheckpoints(operatorId, windowId);
        } catch (IOException ex) {
            logger.error("Error while deleting old checkpoints {}", ex);
        }
    }

    @Override
    public void checkpointed(long l)
    {

    }
}
