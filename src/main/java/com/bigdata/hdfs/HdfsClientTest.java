/**
 * Copyright (C), 2015-2021, XXX有限公司
 * FileName: HdfsClientTest
 * Author:   70312
 * Date:     2021/8/19 14:59
 * Description: 测试类
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 〈测试类〉
 */
public class HdfsClientTest {

    /**
     * 功能描述:
     * 〈HDFS 获取文件系统〉
     *
     */
    public void initHDFS() throws Exception {
        // 创建配置信息对象
        Configuration configuration = new Configuration();
        // 获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        // 打印文件系统
        System.out.println(fs.toString());
    }

    /**
     * 功能描述:
     * 〈HDFS 文件上传（测试参数优先级）〉
     *
     */
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 上传文件
        fs.copyFromLocalFile(new Path("f:/hello.txt"),new Path("/hello5.txt"));

        // 关闭资源
        fs.close();

        System.out.println("over");
    }
}