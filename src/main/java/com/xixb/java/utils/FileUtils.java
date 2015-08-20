package com.xixb.java.utils;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: xixuebin
 * Date: 13-11-8
 * Time: 上午10:17
 */
public class FileUtils {

//    public static void readBigFile() throws Exception{
//        File file = new File(filepath);
//        BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
//        BufferedReader reader = new BufferedReader(new InputStreamReader(fis,"utf-8"),5*1024*1024);// 用5M的缓冲读取文本文件
//
//        String line = "";
//        while((line = reader.readLine()) != null){
//            //TODO: write your business
//        }
//        reader.close();
//    }

    public static boolean delete(File file) {
        if (!file.exists()){
            return true;
        }
        if (file.isFile()) {
            return file.delete();
        } else {
            for (File f : file.listFiles()) {
                delete(f);
            }
            file.delete();
        }
        return true;
    }

    /**
     * 复制文件（夹）到一个目标文件夹
     *
     * @param resFile             源文件（夹）
     * @param objFolderFile 目标文件夹
     * @throws IOException 异常时抛出
     */
    public static void copy(File resFile, File objFolderFile) throws IOException {
        if (resFile==null){
            return ;
        }
        if (!resFile.exists()){
            return;
        }
        if (!objFolderFile.exists()){
            objFolderFile.mkdirs();
        }
        if (resFile.isFile()) {
            File objFile = new File(objFolderFile.getPath() + File.separator + resFile.getName());
            //复制文件到目标地
            InputStream ins = new FileInputStream(resFile);
            FileOutputStream outs = new FileOutputStream(objFile);
            byte[] buffer = new byte[1024 * 512];
            int length;
            while ((length = ins.read(buffer)) != -1) {
                outs.write(buffer, 0, length);
            }
            ins.close();
            outs.flush();
            outs.close();
        } else {
            String objFolder = objFolderFile.getPath() + File.separator + resFile.getName();
            File _objFolderFile = new File(objFolder);
            _objFolderFile.mkdirs();
            if (resFile.listFiles()!=null){
                for (File sf : resFile.listFiles()) {
                    copy(sf, new File(objFolder));
                }
            }
        }
    }

    /**
     * 将文件（夹）移动到令一个文件夹
     *
     * @param resFile             源文件（夹）
     * @param objFolderFile 目标文件夹
     * @throws IOException 异常时抛出
     */
    public static void move(File resFile, File objFolderFile) throws IOException {
        copy(resFile, objFolderFile);
        delete(resFile);
    }

    public static void move(String resPath,String destPath)throws Exception{

        File resFile = new File(resPath);
        if (!resFile.exists()){
            throw new Exception("resFile is not exist!");
        }
        File destFile = new File(destPath);
        if (!destFile.exists()){
            throw new Exception("destFile is not exist!");
        }

        move(new File(resPath),new File(destPath));
    }

}
