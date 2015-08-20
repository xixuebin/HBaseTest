package com.xixb.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Created with IntelliJ IDEA.
 * User: xixuebin
 * Date: 13-10-29
 * Time: 下午2:15
 */
public class TestCom {
    public static Logger logger = Logger.getLogger(TestCom.class);
    public static Configuration conf;
    static {
        conf = new Configuration();
    }

    public static void main(String[] args) throws Exception{
        System.loadLibrary("snappy");
        System.loadLibrary("hadoop");
        // create local file system
        logger.info("for test!!");
        logger.info("java.library.path=" +
                System.getProperty("java.library.path"));
        Path path ;
        if(args!=null && args.length>1){
            path =  new Path(args[0]);
        }else {
            path =  new Path("static/TestFile/aa/b4b3dc5b047a492a8238383da89c4b0f");
        }
        //writeHFile(path);
        readHFile(path);
    }

    public static void writeHFile(Path path) throws IOException{
        FileSystem fs = new RawLocalFileSystem();
        fs.setConf(conf);

//        HFile.Writer hwriter = HFile.getWriterFactoryNoCache(conf).withPath(fs,path)
//                .withCompression(Compression.Algorithm.GZ).create();

        HFile.Writer hwriter = HFile.getWriterFactoryNoCache(conf).withPath(fs,StoreFile.getUniqueFile(fs,
                path))
                .withCompression(Compression.Algorithm.GZ).create();

        //HFile.Writer hwriter = HFile.getWriterFactoryNoCache(conf).withPath(fs,path).create();
        writeSomeRecords(hwriter, 0, 100);
        // close hfile
        hwriter.close();
    }

    private static int writeSomeRecords(HFile.Writer writer, int start, int n)
            throws IOException {
        String localFormatter = "%010d";
        String value = "value";
        for (int i = start; i < (start + n); i++) {
            String key = String.format(localFormatter, i);
            writer.append(Bytes.toBytes(key), Bytes.toBytes(value + key));
        }
        return (start + n);
    }
    public static void readHFile(Path path) throws IOException {
        /**
         * 必须添加,不然会报错:
         * The value of the hbase.metrics.showTableName conf option has not been specified in SchemaMetrics
         * 具体原因不详,参考页面：http://www.pressingquestion.com/4072115/Hbase-Bulk-Load-Exeception
         * 确认为 hbase-0.94.* 版本的一个bug http://chxt6896.github.io/
         */
        //SchemaMetrics.configureGlobally(conf);
        FileSystem fs = new RawLocalFileSystem();
        fs.initialize(URI.create("file:///"), new Configuration());
        fs.setConf(new Configuration());

        CacheConfig cacheConf = new CacheConfig(conf);
        HFile.Reader header = HFile.createReader(fs,path,cacheConf);
        // loadFileInfo
        System.out.print(header.loadFileInfo());


        HFileScanner hscanner = header.getScanner(false, false);
        logger.info("*************************");
        logger.info(header.getPath());
        logger.info(header.getColumnFamilyName());
        logger.info(header.getName());
        logger.info(header.getEntries());
        // seek to the start position of the hfile.
        hscanner.seekTo();
        // print values.
        int index = 1;
        while (hscanner.next()) {
            System.out.println("index: " + index++);
            System.out.println("key: " + hscanner.getKeyString());
            System.out.println("value: " + hscanner.getValueString());
        }
        // close hfile.
        header.close();
    }
}
