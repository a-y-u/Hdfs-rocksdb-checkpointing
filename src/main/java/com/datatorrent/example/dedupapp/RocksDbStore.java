package com.datatorrent.example.dedupapp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;

public class RocksDbStore
{

  public String dbpath;
  private static final int BUFFER_SIZE = 4096;
  String hdfspath;
  private transient RocksDB db;
  private static transient Logger logger = LoggerFactory.getLogger(AdDataDeduper.class);

  public void setHdfspath(String s)
  {
    this.hdfspath = (s);
  }

  public void setDbpath(String s)
  {
    System.out.println(s);
    this.dbpath = (s);
  }

  public static File[] getFileList(String dirPath)
  {
    File dir = new File(dirPath);
    System.out.println(dirPath);
    File[] fileList = dir.listFiles();
    for (File f : fileList) {
      System.out.println(f);
    }
    return fileList;
  }

  public void zipAndSend(long operatorId, long windowId) throws IOException
  {
    FileSystem hdfs = FileSystem.get(new Configuration());
    Path homeDir = hdfs.getHomeDirectory();


    Path hdfsBackupPath = new Path(new Path(hdfs.getHomeDirectory(), hdfspath),
      Long.toString(operatorId));
    Path hdfsCheckpointFilePath = new Path(hdfsBackupPath, "checkpoint_" + windowId + ".zip");
    logger.info("HDFS checkpoint file path {}", hdfsCheckpointFilePath);
    File[] fileList = getFileList(dbpath);

    File localZipFile = new File(dbpath + "/checkpoint_" + windowId + ".zip");//creates a new file for checkpoint
    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(localZipFile));
    byte[] buffer = new byte[1024];

    for (File file : fileList) {
      System.out.println(file.getName());
      ZipEntry e = new ZipEntry(String.valueOf(file));
      out.putNextEntry(e);  //zipping each file inside db
      FileInputStream in = new FileInputStream(file);
      int len;
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);  //writing to the zip entries else file_size=0
      }
      in.close();
      out.closeEntry();

    }
    out.close();
    Path zipPath = new Path(localZipFile.getAbsolutePath());
    logger.info("copy from {} to {}", zipPath, hdfsCheckpointFilePath);
    hdfs.copyFromLocalFile(zipPath, hdfsCheckpointFilePath); //copy zip file from local to hdfs
    localZipFile.delete();
  }

  private void extractFile(ZipInputStream zipIn, File filePath) throws IOException
  {
    System.out.println("in extract file path :" + filePath);
    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));

    byte[] bytesIn = new byte[BUFFER_SIZE];
    int read = 0;
    while ((read = zipIn.read(bytesIn)) != -1) {
      bos.write(bytesIn, 0, read);
    }
    bos.close();
  }

  public void unzip(String zipFilePath, String destDirectory) throws IOException
  {
    System.out.println("dest directory : " + destDirectory);
    File destDir = new File(destDirectory);
    if (!destDir.exists()) {
      destDir.mkdir();
    }
    System.out.println("zip file copied here " + zipFilePath);
    ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
    ZipEntry entry = zipIn.getNextEntry();
    while (entry != null) {
      String filePath = destDirectory + entry.getName();
      System.out.println("inside while loop: " + filePath);
      if (!entry.isDirectory()) {
        System.out.println("inside if loop");
        File f = new File(filePath);
        if (f.exists()) {
          extractFile(zipIn, f);
        }
      }

      System.out.println("entry : " + entry.getName());
      zipIn.closeEntry();
      entry = zipIn.getNextEntry();
    }
    zipIn.close();
  }

  public String setLocalPath(Context context)
  {

    return context.getValue(Context.DAGContext.APPLICATION_ID);
  }

  public RocksDB setDBandFetch(Context.OperatorContext context)
  {
    long operatorId = context.getId();
    try {
      RocksDB.loadLibrary();
      File index = new File(setLocalPath(context));
      index.mkdirs();
      dbpath = (index).getAbsolutePath();

      boolean exists = false;

      FileSystem hdfs = null;
      //if /tmp/rocksbackup already exists, counting the total no of files in tmp/rocskbackup in hdfs
      hdfs = FileSystem.get(new Configuration());
      Path hdfsBackupPath = new Path(new Path(hdfs.getHomeDirectory(), hdfspath),
          Long.toString(operatorId));
      logger.info("HDFS backup path is {}", hdfsBackupPath);
      exists = hdfs.exists(hdfsBackupPath);

      if (!exists) {
        return createFreshDB();
      }

      long ac = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
      Path p = new Path(hdfsBackupPath, "checkpoint_" + ac + ".zip");
      logger.debug("Activation checkpoint file path {}", p);
      if (hdfs.exists(p)) {
        return loadFromHDSFile(hdfs, p);
      } else {
        FileStatus[] files = hdfs.listStatus(hdfsBackupPath);
        Path latestFile = null;
        long latestWid = 0;
        for (FileStatus file : files) {
          if (latestFile == null) {
            latestFile = file.getPath();
            latestWid = getWidFromFilePath(file);
          } else {
            long wid = getWidFromFilePath(file);
            if (wid > latestWid) {
              latestFile = file.getPath();
              latestWid = wid;
            }
          }
        }
        if (latestFile == null) {
          return createFreshDB();
        } else {
          logger.info("latest checkpoint path is {}", latestFile);
          return loadFromHDSFile(hdfs, latestFile);
        }
      }
    } catch (RocksDBException e) {
      throw new RuntimeException("Exception in opening rocksdb", e);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return db;
  }

  RocksDB createFreshDB() throws RocksDBException
  {
    final Options options = new Options().setCreateIfMissing(true).setWriteBufferSize(5096);
    db = RocksDB.open(options, dbpath);
    logger.info("Fresh DB created {} at path {}", db, dbpath);
    return db;
  }

  RocksDB loadFromHDSFile(FileSystem hdfs, Path hdfsFilePpth) throws IOException, RocksDBException
  {
    String fileName = hdfsFilePpth.getName();
    Path inLocal = new Path(dbpath);//Local system path
    hdfs.copyToLocalFile(hdfsFilePpth, inLocal); //copying from HDFS to local
    unzip(inLocal + "/" + fileName, inLocal.toString()); //unziping the required file in local

    //after unzipping,delete the zip file
    Path del = new Path(dbpath + "/" + fileName);
    File index2 = new File(String.valueOf(del));
    index2.delete();

    db = RocksDB.open(dbpath); //opening the DB after the system restarts
    return db;
  }

  long getWidFromFilePath(FileStatus file)
  {
    String[] parts = file.getPath().getName().split("_");
    int idx = parts[1].indexOf(".");
    if (idx > 0) {
      String widStr = parts[1].substring(0, idx);
      long wid = Long.parseLong(widStr);
      return wid;
    }
    return -1;
  }

  public void closeDB()
  {
    if (db != null) {

      db.close();
    }
  }

  public void deleteOlderCheckpoints(long operatorId, long windowId) throws IOException
  {
    FileSystem hdfs = FileSystem.get(new Configuration());
    Path hdfsBackupPath = new Path(new Path(hdfs.getHomeDirectory(), hdfspath),
      Long.toString(operatorId));
    FileStatus[] files = hdfs.listStatus(hdfsBackupPath);
    for (FileStatus file : files) {
      long wid = getWidFromFilePath(file);
      if (wid < windowId) {
        hdfs.delete(file.getPath());
      }
    }
  }

}
