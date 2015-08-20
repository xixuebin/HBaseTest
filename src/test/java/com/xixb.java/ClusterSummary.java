package com.xixb.java;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 5/8/13
 * Time: 2:32 PM
 */
public class ClusterSummary {
    public static Logger logger = Logger.getLogger(ClusterSummary.class);
    public static void main(String[] args) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        ClusterStatus clusterStatus = admin.getClusterStatus();
        System.out.println("request count=" + clusterStatus.getRequestsCount() + ", region count=" + clusterStatus.getRegionsCount());
        for (ServerName info : clusterStatus.getServerInfo()) {
            logger.info("node=" +info.getHostAndPort());
        }
    }
}