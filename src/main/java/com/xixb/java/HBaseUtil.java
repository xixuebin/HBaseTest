package com.xixb.java;

import com.xixb.java.utils.FileUtils;
import com.xixb.java.utils.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.thrift.TException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

/**
 * Created with IntelliJ IDEA.
 * User: xixuebin
 * Date: 13-11-6
 * Time: 上午10:17
 */
public class HBaseUtil {

    public HTable hTable = null;
    public Properties properties = null;

    public HBaseUtil(HTable hTable) {
        this.hTable = hTable;
    }

    public HBaseUtil(ByteBuffer tableName) throws IOException{
        properties = PropertiesUtil.getProperties();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("zookeeper.znode.parent",
                properties.getProperty("zookeeper.znode.parent","/hbase"));
        configuration.set("hbase.zookeeper.property.clientPort",
                properties.getProperty("hbase.zookeeper.property.clientPort","2181"));
        configuration.set("hbase.zookeeper.quorum",
                properties.getProperty("hbase.zookeeper.quorum","xixuebin-vpc"));
        configuration.set("hbase.master",
                properties.getProperty("hbase.master","xixuebin-vpc:60000"));
        System.out.println(configuration.get("hbase.master"));
        hTable = new HTable(configuration, getBytes(tableName));
        hTable.setScannerCaching(500);
    }

    public boolean deleteRowWithStartKeyAndEndKey(ByteBuffer startRow,
                                                  ByteBuffer stopRow){
        try{
            ResultScanner resultScanner = scannerOpenWithStop(startRow, stopRow);
            //List<Delete> deleteList = getDeleteListWithRowKeyScanner(resultScanner);
            return deleteRowWithCount(resultScanner,2000);
        }catch (Exception e){
            e.printStackTrace();
        }

        return true;
    }

    public boolean deleteRowWithCount(ResultScanner resultScanner,int count) throws IOException{
        List<Delete> deleteList = new ArrayList<Delete>();
        int i = 0;
        for (Result res:resultScanner){
            deleteList.add(getRowTs(res.getRow()));
            if (deleteList.size()>count){
                hTable.delete(deleteList);
                deleteList.clear();
                i += deleteList.size();
                System.out.println("data have processed " +i+ " !!!" );
            }
        }
        hTable.delete(deleteList);
        resultScanner.close();
        System.out.println("total count is " +i+ " !!!" );
        return true;
    }
    public List<Delete> getDeleteListWithRowKeyScanner(ResultScanner resultScanner){
        List<Delete> deleteList = new ArrayList<Delete>();
        int count =0;
        for (Result res : resultScanner) {
            System.out.println(count++);
            for (KeyValue kv : res.raw()) {
                deleteList.add(getRowTs(kv.getKey()));
            }
        }
        return deleteList;
    }

    public Delete getRowTs(byte[] rowKey){
        return new Delete(rowKey, HConstants.LATEST_TIMESTAMP, null);
    }

    public ResultScanner scannerOpenWithStop( ByteBuffer startRow,
                                   ByteBuffer stopRow) throws IOError, TException {
        try {
            Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
            scan.setCaching(500);
            scan.setCacheBlocks(false);
            scan.setFilter(new KeyOnlyFilter());
            return hTable.getScanner(scan);
        } catch (IOException e) {
            throw new IOError(e.getMessage());
        }
    }

    public static ByteBuffer getStringByteBuffer(String sValue){
          return ByteBuffer.wrap(sValue.getBytes());
    }


    public void deleteRowFromFile() throws IOException{
        String confPath = properties.getProperty("zookeeper.znode.parent","/hbase");
        File file = new File(confPath);
        if (file.isDirectory() && file.exists()){
            for (File f:file.listFiles()){
                BufferedInputStream fis = new BufferedInputStream(new FileInputStream(f));
                BufferedReader reader = new BufferedReader(new InputStreamReader(fis),5*1024*1024);// 用5M的缓冲读取文本文件
                String rowKey;
                List<Delete> deleteList = new ArrayList<Delete>();
                while((rowKey = reader.readLine()) != null){
                    deleteList.add(getRowTs(rowKey.getBytes()));
                    if (deleteList.size()>2000){
                        hTable.delete(deleteList);
                    }
                }
                reader.close();
                fis.close();
                hTable.delete(deleteList);
                FileUtils.move(f, new File(confPath+"deleted"+f.getName()));
            }
        }
    }
    public static void main(String[] args) throws Exception{

        String tableName = "store_detail_2";
        String startKey = "0000";
        String endKey = "1111";
        if(args.length>3){
            tableName = args[0];
            startKey = args[1];
            endKey = args[2];
        }else {
            System.out.println("wrong param!!");
            System.out.println("use default param!!");
        }
        long begin = System.currentTimeMillis();
        HBaseUtil hBaseUtil = new HBaseUtil(getStringByteBuffer(tableName));
        hBaseUtil.deleteRowWithStartKeyAndEndKey(getStringByteBuffer(startKey),
                getStringByteBuffer(endKey));
        System.out.println("cost total time is :"+(System.currentTimeMillis()-begin));
    }

}
