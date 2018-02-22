
package com.datatorrent.example.dedupapp;


import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;
import org.rocksdb.Options;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Env;
import org.rocksdb.util.Environment;
import org.rocksdb.RocksObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import java.io.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.rocksdb.Checkpoint;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static com.datatorrent.common.util.FSStorageAgent.kryo;

public class RocksDBOutputOperator extends BaseOperator implements CheckpointNotificationListener {
    private boolean sendPerTuple = true;
    private static transient Logger logger = LoggerFactory.getLogger(RocksDBOutputOperator.class);
    public final transient DefaultOutputPort<String> output  = new DefaultOutputPort<>();
    private transient RocksDB db;

    String dbpath = "/tmp/db"; //our database path in local file system

    Checkpoint checkpoint;
    CheckpointNotificationListener checkpointListener;
    BackupEngine backupEngine;
    Env env ;

    int count = 1 ;

    public static Path getAllFilePathhdfs(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        // will return the latest file in HDFS
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        long max_modified=0;
        Path fileToGet = null;
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.getModificationTime()>max_modified) {
                max_modified=fileStat.getModificationTime();
                fileToGet=fileStat.getPath();
                System.out.println(fileToGet);
            }
        }
        return fileToGet;
    }

    private static File[] getFileList(String dirPath) {
        File dir = new File(dirPath);
        System.out.println(dirPath);
        File[] fileList = dir.listFiles();
        for (File f : fileList){
            System.out.println(f);
        }
        return fileList;
    }

    private static File[] getFileListZip(String dirPath) {
        File dir = new File(dirPath);

        File[] fileList = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".zip");
            }
        });
        return fileList;
    }

    public void deposit() throws IOException {
        FileSystem hdfs =FileSystem.get(new Configuration());
        Path homeDir=hdfs.getHomeDirectory();
        System.out.println("home directory "+homeDir); //hdfs home directory

        Path newFolderPath=new Path(String.valueOf(homeDir)+"/tmp/rocksbackup"+"/checkpoint_"+count);
        Path newFilePath=new Path(String.valueOf(newFolderPath)+".zip"); //creates a file path in hdfs for checkpoint.zip
        hdfs.createNewFile(newFilePath);

        Path localFilePath = new Path(dbpath);
        File[] fileList = getFileList(dbpath);

        File f = new File(dbpath+"/checkpoint_"+count+".zip"); //creates a new file for checkpoint
        count++;
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
        System.out.println();
        byte[] buffer = new byte[1024];

        for(File file : fileList) {
            if(!(file.getName().endsWith(".zip")||file.getName().startsWith("LOG.old.")))
            //skip zipping .zip file to avoid recursion
            {
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
        }
        out.close();
        System.out.println("zip size "+f.length());

        System.out.println("new hdfs file path "+newFilePath);

        Path zipPath = new Path(dbpath+"/checkpoint_"+(count-1)+".zip");
        System.out.println("zip file path to be deleted "+zipPath);

        hdfs.copyFromLocalFile(zipPath, newFilePath); //copy zip file from local to hdfs

        File[] delList = getFileListZip(dbpath);
        for(File file:delList)
            file.delete();   //delete checkpoint zip file from local


        System.out.println("deleted checkpoint "+(count-1) );

    }

    private static final int BUFFER_SIZE = 4096;

    private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }

    public void unzip(String zipFilePath, String destDirectory) throws IOException {
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdir();      //Db did not exist after restarting hence create
        }
        ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath));
        ZipEntry entry = zipIn.getNextEntry();
        // iterates over entries in the zip file
        while (entry != null) {
            String filePath = entry.getName();

            if (!entry.isDirectory()) {

                extractFile(zipIn, filePath);
            }
            zipIn.closeEntry();
            entry = zipIn.getNextEntry();
        }
        zipIn.close();
    }


    @Override
    public void beforeCheckpoint(long windowId) {

        logger.info("   Before Checkpoint {} ,{}",db.getSnapshot(),windowId);
        FileSystem hdfs = null;
        try {
            hdfs =FileSystem.get(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            System.out.println("giving path");
            //backupableDBOptions =new BackupableDBOptions("/tmp/rocks");

            System.out.println("path given");

            backupEngine.createNewBackup(db,true);
            System.out.println("created backup");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

    }



    @Override
    public void setup(OperatorContext context) {

        File index = new File("/tmp/db");
        if (index.exists()) { // if /tmp/db path already exists

            String[] entries = index.list();
            if (entries.length != 0) { // if there are files inside /tmp/db
                for (String s : entries) {
                    //deleting each file inside it as you cant delete a directory with content inside it
                    File currentFile = new File(index.getPath(), s);
                    currentFile.delete();

                }
            }
            try {// deleting the db folder
                FileUtils.deleteDirectory(new File(String.valueOf(index)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        RocksDB.loadLibrary();

        File index1=new File("/tmp/rocks");
        if (index1.exists()) {
            try {
                index.mkdirs();
                RestoreOptions restoreOptions = new RestoreOptions(true);
                //this.env=options.getEnv();
                System.out.println("inside if loop ");
                BackupableDBOptions backupableDBOptions;
                backupableDBOptions =new BackupableDBOptions("/tmp/rocks");
                //BackupEngine backupEngine;
                this.backupEngine = backupEngine.open(env,backupableDBOptions);
                backupEngine.restoreDbFromLatestBackup("/tmp/db", "/tmp/db", restoreOptions);
                db = RocksDB.open(dbpath); //after the system restarts,opening the DB
                System.out.println("db opened in if loop");
            } catch (RocksDBException e) {
                e.printStackTrace();
                System.out.println(e);
            }
            //start the checkpoint count from the no retrieved from the latest file unzipped
            //count = Integer.parseInt(hdfsFile.getName().replaceAll("\\D+",""));

        }

        else {
            //creating /temp/db

            File file = new File(dbpath);
            file.mkdirs();
            logger.info("File created {}", file);

            try (final Options options = new Options().setCreateIfMissing(true).setWriteBufferSize(5096)) {
                db = RocksDB.open(options, dbpath);
                logger.info("DB created {}", db);
                this.env=options.getEnv();
                index1.mkdirs();
                BackupableDBOptions backupableDBOptions;
                backupableDBOptions =new BackupableDBOptions("/tmp/rocks");
                this.backupEngine = backupEngine.open(env,backupableDBOptions);
            } catch (RocksDBException e) {
                throw new RuntimeException("Exception in opening rocksdb", e);
            }
        }

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
                    Tuple t = (Tuple) toObject(retvalue); // storing the value in a byte array
                    if(t.getAmount()<tuple.getAmount()) { //comparing the bid values
                        db.put(key, value);
                        System.out.println("Greater amount for " + s);
                        System.out.println("old bid amount " + t.getAmount());
                        System.out.println("congo,youve got the bid at "  +tuple.getAmount());
                        //logger.info("congo,youve got the bid at {}"  ,tuple.getAmount());
                    }
                    else {
                        System.out.println("Bid lost for " + s + "coz " + t.getAmount() + ">" + tuple.getAmount());

                        //logger.info("bid lost");
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


        if (db != null) {

            db.close();
        }
    }

    @Override
    public void checkpointed(long l) {

    }

    @Override
    public void committed(long l) {
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(new Configuration());
            Path homeDir=hdfs.getHomeDirectory();
            Path newFolderPath=new Path(String.valueOf(homeDir)+"/tmp/rocksbackup"+"/checkpoint_"+(count-3)+".zip");
            hdfs.delete(newFolderPath,true);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            backupEngine.purgeOldBackups(5);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }


    }
}

