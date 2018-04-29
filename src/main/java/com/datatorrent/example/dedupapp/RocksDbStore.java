package com.datatorrent.example.dedupapp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.*;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;


import static org.slf4j.LoggerFactory.getLogger;

public class RocksDbStore
{
  private static final Logger LOG = getLogger(RocksDbStore.class);

  public String dbpath;
  private static final int BUFFER_SIZE = 1024;
  String hdfspath;
  private transient RocksDB db;
  private static transient Logger logger = getLogger(AdDataDeduper.class);
  RocksIterator ri;
  int count=0;
  String hdfsBackupPath;

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
    logger.info("inside getfilelist");
    File[] fileList = dir.listFiles();
    return fileList;
  }


  public static FileStatus[] getFileListFromHDFS(Path dirPath) throws IOException {


      Configuration conf = new Configuration();
      FileSystem fs = null;
      try {
          fs = FileSystem.get(new URI(String.valueOf(dirPath)), conf);
      } catch (IOException e) {
          e.printStackTrace();
      } catch (URISyntaxException e) {
          e.printStackTrace();
      }
      FileStatus[] fileStatus = fs.listStatus(dirPath);
      for(FileStatus status : fileStatus){
       logger.info("found {}",status.getPath().toString());

    }
      return fileStatus;
  }

  public void zipAndSend(long operatorId, long windowId) throws IOException
  {
    List<File> newFiles = incremental_backup(windowId);
    LOG.info("new files {}", newFiles);
    copyFile(operatorId, windowId, newFiles);

  }


  Map<String, Long> fileTimeStampMap = new HashMap<>();

  public List<File> incremental_backup(long windowId) throws IOException {
    List<File> newFiles = new ArrayList<>();
    File[] files = getFileList(dbpath);
    for (File file : files) {
      Long lastTimestamp = fileTimeStampMap.get(file.getName());
      if (lastTimestamp == null) {
          if(file.getName().startsWith("MANIFEST")){
              logger.info("file name {}",file.getName());
              deleteOPTorManifest("MANIFEST");
          }
          else if(file.getName().startsWith("OPTIONS")){
            logger.info("file name {}",file.getName());
            deleteOPTorManifest("OPTIONS");
          }
          else if(file.getName().startsWith(".")){
            logger.info("file name {}",file.getName());
            deleteOPTorManifest(".");
          }

        LOG.info("new file {}", file.getName());
        newFiles.add(file);
        fileTimeStampMap.put(file.getName(), file.lastModified());
        continue;
      }
      if (lastTimestamp < file.lastModified()) {
        LOG.info("Modified file {}", file.getName());
        newFiles.add(file);
        fileTimeStampMap.put(file.getName(), file.lastModified());
      }
    }



    return newFiles;
  }

    private void deleteOPTorManifest(String name) throws IOException {


        logger.info("{} has to be deleted ",name);
        Path hdfsBackupPath = new Path(this.hdfsBackupPath);

        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(String.valueOf(hdfsBackupPath)), conf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        logger.info("path is {}",hdfsBackupPath);
        File check  =  new File(String.valueOf(hdfsBackupPath));
        logger.info("check {}",fs.exists(hdfsBackupPath));

        if(fs.exists(hdfsBackupPath)){


          FileStatus[] files  = getFileListFromHDFS(hdfsBackupPath);
          logger.info("second time i should be here {}",files.length);
            for (FileStatus file : files) {
                logger.info("files in  h d f s {}",file.getPath());
                logger.info("file name checked for {}",file.getPath());
                if (file.getPath().getName().startsWith(name)) {
                    logger.info("delete this : {}", file.getPath());
                    fs.delete(file.getPath(), false);
                }
            }
        }


    }

    private void copyFile(long operatorId, long wid, List<File> files) throws IOException
  {
    FileSystem hdfs = FileSystem.get(new Configuration());
    Path homeDir = hdfs.getHomeDirectory();

    Path hdfsBackupPath = new Path(new Path(hdfs.getHomeDirectory(), hdfspath),
      Long.toString(operatorId));

    for (File file: files) {
      try (InputStream in = new FileInputStream(file);
        OutputStream out = hdfs.create(new Path(hdfsBackupPath, file.getName()))) {
        IOUtils.copy(in, out);
      } catch (Exception ex) {
        LOG.error("Error while copying {}", ex);
      }
    }
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

      this.hdfsBackupPath=String.valueOf(hdfsBackupPath);
      logger.info("HDFS backup path is {}", hdfsBackupPath);
      exists = hdfs.exists(hdfsBackupPath);

      if (!exists) {
        logger.info("New DB created (SHOULD NEVER HAPPEN(ONLY ONCE))");
        return createFreshDB();
      }



      long ac = context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);
      System.out.println("path "+hdfsBackupPath);
      logger.info("Activation checkpoint file path {}",hdfsBackupPath);
      if (hdfs.exists(hdfsBackupPath)) {
         logger.info("in if: {}",hdfsBackupPath);
        return loadFromHDSFile(hdfs,hdfsBackupPath);
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
        logger.info("latestFile value {}",latestFile);
        if (latestFile == null) {
          logger.info("New DB created ");
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
      logger.info("inlocal path {}",inLocal);
      logger.info("filename in loadHdfsFile {}",String.valueOf(hdfsFilePpth));
       FileStatus[] files  = getFileListFromHDFS(hdfsFilePpth);
      logger.info("files {}",files);
    for(FileStatus e :files){
      if(!e.isDirectory()) {
        System.out.println("file from hdfs : " + e.getPath());

        hdfs.copyToLocalFile(false,e.getPath(), inLocal,true);
        logger.info("inside loop : inlocal {}", inLocal);
        System.out.println(" " + e + " copied");
      }

    }
    db = RocksDB.open(dbpath);//opening the DB after the system restarts
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

  public void current_stat(long operatorId) throws IOException {
    FileSystem hdfs = null;
    hdfs = FileSystem.get(new Configuration());
    Path hdfsBackupPath = new Path(new Path(hdfs.getHomeDirectory(), hdfspath), Long.toString(operatorId));
    File[] files = getFileList(dbpath);
    ArrayList<String> fileInLocal = new ArrayList<String>();
    for(File e1 : files){
      fileInLocal.add(e1.getName());
    }
    logger.info("files in local {}",fileInLocal);


    FileStatus[] fileStatuses = getFileListFromHDFS(hdfsBackupPath);
    ArrayList<String> fileInHdfs = new ArrayList<String>();
    for(FileStatus e : fileStatuses){
      fileInHdfs.add(e.getPath().getName());
    }

    logger.info("files in hdfs {}",fileInHdfs);

    for(FileStatus e : fileStatuses){
      if(!fileInLocal.contains(e.getPath().getName())){

        hdfs.delete(e.getPath());
        logger.info("deleted {}",e);
      }
    }
  }

}
