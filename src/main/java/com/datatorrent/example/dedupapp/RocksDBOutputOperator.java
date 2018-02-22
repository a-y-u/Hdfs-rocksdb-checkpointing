
package com.datatorrent.example.dedupapp;



import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.BaseOperator;

import org.rocksdb.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import java.io.*;
import java.io.IOException;


import org.rocksdb.Checkpoint;



public class RocksDBOutputOperator extends BaseOperator implements CheckpointNotificationListener  {

    //private static final int BUFFER_SIZE = 4096;
    private boolean sendPerTuple = true;
    private static transient Logger logger = LoggerFactory.getLogger(RocksDBOutputOperator.class);
    public final transient DefaultOutputPort<String> output  = new DefaultOutputPort<>();
    private transient RocksDB db;

    //our database path in local file system

    Checkpoint checkpoint;
    CheckpointNotificationListener checkpointListener;

    int count = 1 ;
    RocksDbStore DBstore =new RocksDbStore() ;


    @Override
    public void beforeCheckpoint(long windowId) {
        System.out.println("committed ID : "+windowId);
        logger.info("   Before Checkpoint {} ,{}",db.getSnapshot(),windowId);

        try {

            DBstore.zipAndSend(windowId); //zipping db and pushing into hdfs

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void setup(OperatorContext context) {
        System.out.println("inside setup ");
        DBstore.setDbpath("/tmp/db");
        DBstore.setHdfspath("/tmp/rocksbackup/");
        db = DBstore.setDBandFetch(context);
    }

    public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
        // converting byte array into an object
        Object obj = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {

            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            obj = (Tuple)ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
        return obj;
    }

    public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>() {

        @Override
        public void process(Tuple tuple) {

            logger.info("Processing tuple {}", tuple);
            String s = tuple.makeKey(); //converting key into String format
            byte[] key = s.getBytes();  // converting the key into byte format FOR ROCKSDB
            try {
                byte[] retvalue = db.get(key); //retvalue contains the key
                if (retvalue == null) { // unique if key not found in database
                    byte[] value = tuple.makeValue(); // storing the value in a byte array
                    db.put(key, value);
                    logger.info("Unique key  inserted {} {} {}",s,tuple.getAmount(),tuple.getClient_id());
                } else { // duplicate if key(page_id site_id) match found in database
                    byte[] value = tuple.makeValue(); //

                    Tuple t= (Tuple) toObject(retvalue); // storing the value in a byte array
                    if(t.getAmount()<tuple.getAmount()) { //comparing the bid values
                        db.put(key, value);
                        System.out.println("Greater amount for " + s);
                        System.out.println("old bid amount " + t.getAmount());
                        System.out.println("congo,youve got the bid at "  +tuple.getAmount());

                    }
                    else {
                        System.out.println("Bid lost for " + s + "coz " + t.getAmount() + ">" + tuple.getAmount());
                    }
                }
            } catch (RocksDBException e) {
                throw new RuntimeException("Exception in getting key from rocksdb", e);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.out.println("class not found");
            }

        }
    };

    @Override
    public void endWindow(){

    }

    @Override
    public void teardown() {
        DBstore.closeDB();
    }

    @Override
    public void checkpointed(long windowId) {

    }

    @Override
    public void committed(long windowId) {

    }


}
