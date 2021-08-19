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

/**
 * 〈HdfsClient 类〉
 */
public class HdfsClient {

    // 上传文件
    public static void main(String[] args) {

        // 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);

    }
}