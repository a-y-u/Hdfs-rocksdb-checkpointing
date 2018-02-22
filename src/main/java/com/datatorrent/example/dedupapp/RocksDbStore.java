package com.datatorrent.example.dedupapp;

import com.datatorrent.api.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class RocksDbStore {


    public String dbpath;
    private static final int BUFFER_SIZE = 4096;
    String pt;
    String hdfspath;
    private transient RocksDB db;
    private static transient Logger logger = LoggerFactory.getLogger(RocksDBOutputOperator.class);

    public void setHdfspath(String s){
        this.hdfspath=(s);
    }

    public void setDbpath(String s){
        System.out.println(s);
        this.dbpath = (s);
    }

    public static File[] getFileList(String dirPath) {
        File dir = new File(dirPath);
        System.out.println(dirPath);
        File[] fileList = dir.listFiles();
        for (File f : fileList){
            System.out.println(f);
        }
        return fileList;
    }
    public void zipAndSend(long windowId) throws IOException {
        FileSystem hdfs =FileSystem.get(new Configuration());
        Path homeDir=hdfs.getHomeDirectory();
        System.out.println("home directory "+homeDir); //hdfs home directory

        Path newFolderPath=new Path((homeDir).toString()+"/tmp/rocksbackup"+"/checkpoint_"+windowId);
        Path newFilePath=new Path((newFolderPath).toString()+".zip"); //creates a file path in hdfs for checkpoint.zip
        hdfs.createNewFile(newFilePath);

        Path localFilePath = new Path(dbpath);
        File[] fileList = getFileList(dbpath);

        File f = new File(dbpath+"/checkpoint_"+windowId+".zip");//creates a new file for checkpoint
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
        byte[] buffer = new byte[1024];

        for(File file : fileList) {

            System.out.println(file.getName());

            ZipEntry e = new ZipEntry(String.valueOf(file));
            out.putNextEntry(e);  //zipping each file inside db

            FileInputStream in = new FileInputStream(file);
            int len;
            while ((len = in.read(buffer)) > 0)
            {
                out.write(buffer, 0, len);  //writing to the zip entries else file_size=0
            }
            in.close();
            out.closeEntry();

        }
        out.close();
        Path zipPath = new Path(dbpath+"/checkpoint_"+windowId+".zip");
        hdfs.copyFromLocalFile(zipPath, newFilePath); //copy zip file from local to hdfs
        File file = new File(dbpath+"/checkpoint_"+windowId+".zip");
        file.delete();
    }

    private void extractFile(ZipInputStream zipIn, File filePath) throws IOException {
        System.out.println("in extract file path :"+filePath);
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));

        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }

    public void unzip(String zipFilePath, String destDirectory) throws IOException {
        System.out.println("dest directory : "+destDirectory);
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();
        }
        System.out.println("zip file copied here " + zipFilePath);
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
        ZipEntry entry = zipIn.getNextEntry();
        while (entry != null) {
            String filePath =  destDirectory+entry.getName();
            System.out.println("inside while loop: "+filePath);
            if (!entry.isDirectory()) {
                System.out.println("inside if loop");
                File f = new File(filePath);
                if(f.exists()) {
                    extractFile(zipIn, f);
                }
            }

            System.out.println("entry : "+entry.getName());
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
        }
        zipIn.close();
    }

    public String setLocalPath(Context context){

        return  context.getValue(Context.DAGContext.APPLICATION_ID);
    }

    public void setHdfsBackupPath(String s){
        FileSystem hdfs = null;
        Path path=null;
        try {
            hdfs =FileSystem.get(new Configuration());
            path = new Path(hdfs.getHomeDirectory().toString()+s);
            pt = path.toString();
            System.out.println("hdfs path: "+pt);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getHdfsBackupPath(){
        return pt;
    }
    public RocksDB setDBandFetch(Context context){


        try {

            File index = new File(setLocalPath(context));

            dbpath = (index).getAbsolutePath();

            boolean exists = false;
            long fileCount = 0;

            ContentSummary cs;
            FileSystem hdfs = null;
            Path createPt = null;

            //if /tmp/rocksbackup already exists, counting the total no of files in tmp/rocskbackup in hdfs
            hdfs = FileSystem.get(new Configuration());
            setHdfsBackupPath(hdfspath);
            createPt = new Path(getHdfsBackupPath());
            exists = hdfs.exists(createPt);

            if (exists) {
                cs = hdfs.getContentSummary(createPt);
                fileCount = cs.getFileCount();
            } else {
                boolean made = hdfs.mkdirs(createPt);
            }

            RocksDB.loadLibrary();

            if (fileCount != 0) {

                long ac= context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID);


                Path p = new Path(pt + "/checkpoint_" +ac + ".zip");

                //HDFS path
                System.out.println("hdfs checkpoint file in hdfs" + String.valueOf(p));

                File file = new File(String.valueOf(index));
                file.mkdirs();

                Path inLocal = new Path(dbpath);//Local system path
                System.out.println("hdfs checkpoint file in hdfs :" + String.valueOf(p)+" inlocal : "+inLocal.toString());
                hdfs.copyToLocalFile(p, inLocal); //copying from HDFS to local
                System.out.println("destDirectory : "+String.valueOf(inLocal));
                unzip(inLocal + "/checkpoint_" + ac+ ".zip", inLocal.toString()); //unziping the required file in local

                //after unzipping,delete the zip file
                Path del = new Path(dbpath + "/checkpoint_" + ac+ ".zip");
                File index2 = new File(String.valueOf(del));
                index2.delete();

                db = RocksDB.open(dbpath); //opening the DB after the system restarts


            } else {

                File file = new File(String.valueOf(index));
                file.mkdirs();
                System.out.println("db made");
                logger.info("File created {}", file);

                final Options options = new Options().setCreateIfMissing(true).setWriteBufferSize(5096);
                db = RocksDB.open(options, dbpath);
                logger.info("DB created {}", db);
                System.out.println("rocksdb created");
            }
        }
        catch(RocksDBException e){
            throw new RuntimeException("Exception in opening rocksdb", e);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return db;
    }

    public void closeDB() {
        if (db != null) {

            db.close();
        }
    }
}
