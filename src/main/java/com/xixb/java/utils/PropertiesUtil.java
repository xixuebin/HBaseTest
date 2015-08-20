package com.xixb.java.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.log4j.Logger.getLogger;

/**
 * Author: xixuebin
 * CreateTime: 13-8-20 上午10:14
 * Company:si-tech www.si-tech.com.cn
 */
public class PropertiesUtil {

    static Logger logger = getLogger(PropertiesUtil.class);

    private static Properties properties= new Properties();

    private static final String PROPERTIES_FILE_NAME="conf.properties";

    /**
     * init Properties
     */
    private static void initProperties(String confPath){
        try {
            //InputStream ips = PropertiesUtil.class.getResourceAsStream(PROPERTIES_FILE_NAME);
            InputStream ips =  Thread.currentThread().getContextClassLoader().getResourceAsStream(confPath);
            properties.load(ips);
            ips.close();
        } catch (IOException e) {
            logger.error("init Properties error");
            logger.error(e.getMessage());
        }
    }

    /**
     * get an properties instance
     * @return  Properties
     */
    public static Properties getProperties(){
        return getProperties(PROPERTIES_FILE_NAME);
    }

    public static Properties getProperties(String confPath){
        if (confPath==null||confPath.length()==0){
            logger.info("wrong conf path");
            throw new IllegalArgumentException("wrong conf path");
        }
        if (properties.isEmpty()){
            initProperties(confPath);
        }
        return properties;
    }


    public static void main(String[] args){
            logger.info(getProperties().getProperty("hbase.zookeeper.quorum","xixuebin-vpc"));
    }

}
