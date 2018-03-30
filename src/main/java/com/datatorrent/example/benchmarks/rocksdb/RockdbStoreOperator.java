package com.datatorrent.example.benchmarks.rocksdb;

import java.io.IOException;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.hdht.MutableKeyValue;
import com.datatorrent.example.dedupapp.RocksDbStore;
import com.datatorrent.netlet.util.Slice;

import static org.slf4j.LoggerFactory.getLogger;

public class RockdbStoreOperator extends BaseOperator implements Operator.CheckpointNotificationListener
{
  private static final Logger LOG = getLogger(RockdbStoreOperator.class);

  private transient RocksDbStore store;
  private RocksDbStore DBstore = new RocksDbStore();
  private transient RocksDB db;
  private int operatorId;

  public transient DefaultInputPort<MutableKeyValue> inputPort = new DefaultInputPort<MutableKeyValue>()
  {
    @Override
    public void process(MutableKeyValue o)
    {
      try {
        db.put(o.getKey(), o.getValue());
      } catch (RocksDBException e) {
        LOG.error("Error while trying to write to rocksdb");
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    DBstore.setHdfspath("/tmp/rocksbackup/");
    db = DBstore.setDBandFetch(context);
    if (db == null) {
      throw new RuntimeException("Unable to initialize db ");
    }
    operatorId  = context.getId();
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
    LOG.info("Before Checkpoint {} windowId = {}", db.getSnapshot(), windowId);
    try {
      DBstore.zipAndSend(operatorId, windowId); //zipping db and pushing into hdfs
    } catch (IOException e) {
      LOG.error("Error while taking checkpoint {}", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkpointed(long l)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      DBstore.deleteOlderCheckpoints(operatorId, windowId);
    } catch (IOException ex) {
      LOG.error("Error while deleting old checkpoints {}", ex);
    }
  }
}
