
package com.datatorrent.example.dedupapp;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.common.util.BaseOperator;

public class RocksDBOutputOperator extends BaseOperator implements CheckpointNotificationListener
{

  //private static final int BUFFER_SIZE = 4096;
  private boolean sendPerTuple = true;
  private static transient Logger logger = LoggerFactory.getLogger(RocksDBOutputOperator.class);
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
  private transient RocksDB db;

  //our database path in local file system

  Checkpoint checkpoint;
  CheckpointNotificationListener checkpointListener;

  int count = 1;
  RocksDbStore DBstore = new RocksDbStore();

  @Override
  public void beforeCheckpoint(long windowId)
  {
    logger.info("Before Checkpoint {} windowId = {}", db.getSnapshot(), windowId);
    try {
      DBstore.zipAndSend(operatorId, windowId); //zipping db and pushing into hdfs
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  int operatorId;

  @Override
  public void setup(OperatorContext context)
  {
    logger.info("inside setup ");
    DBstore.setHdfspath("/tmp/rocksbackup/");
    db = DBstore.setDBandFetch(context);
    if (db == null) {
      throw new RuntimeException("Unable to initialize db ");
    }
    operatorId = context.getId();
  }

  public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException
  {
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

  public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
  {

    @Override
    public void process(Tuple tuple)
    {

      logger.trace("Processing tuple {}", tuple);
      String s = tuple.makeKey(); //converting key into String format
      byte[] key = s.getBytes();  // converting the key into byte format FOR ROCKSDB
      try {
        byte[] retvalue = db.get(key); //retvalue contains the key
        if (retvalue == null) { // unique if key not found in database
          byte[] value = tuple.makeValue(); // storing the value in a byte array
          db.put(key, value);
          logger.trace("Unique key  inserted {} {} {}", s, tuple.getAmount(), tuple.getClient_id());
        } else { // duplicate if key(page_id site_id) match found in database
          byte[] value = tuple.makeValue(); //

          Tuple t = (Tuple)toObject(retvalue); // storing the value in a byte array
          if (t.getAmount() < tuple.getAmount()) { //comparing the bid values
            db.put(key, value);
            logger.debug("Greater amount for {} old bid amount {}  {}",  s, t.getAmount(), tuple.getAmount());
          } else {
            Log.trace("Bid lost for " + s + "coz " + t.getAmount() + ">" + tuple.getAmount());
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
  public void endWindow()
  {

  }

  @Override
  public void teardown()
  {
    DBstore.closeDB();
  }

  @Override
  public void checkpointed(long windowId)
  {

  }

  @Override
  public void committed(long windowId)
  {

  }

}
