/**
 * Copyright (C), 2015-2021, XXX有限公司
 * FileName: HdfsClient
 * Author:   70312
 * Date:     2021/8/19 10:15
 * Description: HdfsClient 类
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 〈HdfsClient 类〉
 */
public class HdfsClient {

    // 上传文件
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        // 配置Hadoop用户名
        System.setProperty("HADOOP_USER_NAME","bigdata");
        // 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
//      FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "bigdata");
        // hadoop100为192.168.1.100
        configuration.set("fs.defaultFS", "hdfs://hadoop100:9000");
        FileSystem fs = FileSystem.get(configuration);
        //上传文件
        fs.copyFromLocalFile(new Path("f:/hello.txt"),new Path("/hello2.txt"));
        // 关闭资源
        fs.close();
        System.out.println("over");
    }

}