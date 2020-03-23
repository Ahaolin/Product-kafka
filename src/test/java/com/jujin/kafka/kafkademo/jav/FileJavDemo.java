package com.jujin.kafka.kafkademo.jav;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存 jav 文件目录
 */
public class FileJavDemo {

    /**
     * 保存Jav的目录{@value}
     */
    private final static String FILEPATH = "F:\\迅雷下载\\MOVIE";

    /**
     * 需要新建文件的目录
     */
    private final static String BASEPATH = "E:\\迅雷下载";

    /**
     * 用于过滤{@see com.jujin.kafka.kafkademo.jav.FileJavDemo#FILEPATH} 中的文件加
     */
    private static List<String> fileNameList = Arrays.asList( "欧美", "日韩");

    public static void main(String[] args) {
        if (fileNameList == null || fileNameList.isEmpty()) {
            throw new RuntimeException("需要设置保存的文件夹");
        }

        Set<String> pathSet = new HashSet<>();

        fileNameList.forEach((fileName) -> {
            // 拼接原先的File
            File orginFile = new File(FILEPATH + File.separator + fileName);
            if (orginFile.exists()) {
                File[] files = orginFile.listFiles();
                if (files.length > 0) {
                    for (File file : files) {
                        if (file.isDirectory()) {
                            File[] sonFiles = file.listFiles();
                            for (File sonFile : sonFiles) {
                                if (file.isDirectory()){
                                    pathSet.add(file.getName() + File.separator + sonFile.getName());
                                }else {
                                    pathSet.add(file.getName());
                                }
                            }

                        }
                    }
                }
            }
            System.out.println(pathSet);
            pathSet.forEach(v -> createDirs(BASEPATH + File.separator + fileName + File.separator + v));
            pathSet.clear();
        });

    }


    public static void createDirs(String filePath) {
        File baseFile = new File(filePath);
        if (!baseFile.exists()) {
            System.out.println("创建 文件夹 ：" + filePath);
            baseFile.mkdirs();
        }
    }


}
