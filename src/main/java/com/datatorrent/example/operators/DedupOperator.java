package com.datatorrent.example.operators;

import java.io.IOException;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.example.dedupapp.RocksDbStore;

public abstract class DedupOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private static transient Logger logger = LoggerFactory.getLogger(DedupOperator.class);
  private transient RocksDB db;

  public final transient DefaultOutputPort<Object> uniqueData = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<Object> duplicateData = new DefaultOutputPort<>();
  private final static byte[] EMPTY_VAL = new byte[]{0};

  @AutoMetric
  public long totalDedups = 0;
  @AutoMetric
  public long totalUnique = 0;

  public final transient DefaultInputPort<Object> inputPort = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      try {
        processTuple(o);
      } catch (RocksDBException e) {
        logger.error("Exception while processing tuple {} ex {}", o, e);
      }
    }
  };

  private RocksDbStore DBstore = new RocksDbStore();
  long operatorId;
  public abstract byte[] getKey(Object o);
  protected void processTuple(Object o) throws RocksDBException
  {
    byte[] key = getKey(o);
    byte[] retvalue = db.get(key); //retvalue contains the key
    if (retvalue == null) {
      totalUnique++;
      logger.info("Unique inserted");
      uniqueData.emit(o);
      db.put(key, EMPTY_VAL);
    } else {
      totalDedups++;
      logger.info("Duplicate found");
      duplicateData.emit(o);
    }
  }
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
    logger.info("total uniques : {}",totalUnique);
    logger.info("total duplicates : {}",totalDedups);

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
      DBstore.current_stat(operatorId);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void checkpointed(long l)
  {

  }

}
