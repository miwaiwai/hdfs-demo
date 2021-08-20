/**
 * Copyright (C), 2015-2021, XXX有限公司
 * FileName: HdfsClientIOTest
 * Author:   70312
 * Date:     2021/8/19 18:33
 * Description: 通过io操作hdfs的测试类
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 〈通过io操作hdfs的测试类〉
 */
public class HdfsClientIOTest {
    
    /**
     * @Author 70312
     * @Description hdfs文件上传
     * @Date 18:44 2021/8/19
     * @Param * @param null
     * @return
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 创建输入流
        FileInputStream fis = new FileInputStream(new File("f:/hadoop-2.7.2.tar.gz"));

        // 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hadoop-2.7.2.tar.gz"));

        // 流对拷
        IOUtils.copyBytes(fis,fos,configuration);

        // 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

        System.out.println("over");
    }

    /**
     * @Author 70312
     * @Description HDFS文件下载
     * @Date 8:48 2021/8/20
     * @Param * @param null
     * @return
     */
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hello4.txt"));

        // 获取输出流

        // 流对拷
        IOUtils.copyBytes(fis,System.out,configuration);

        // 关闭资源
        IOUtils.closeStream(fis);
        System.out.println("over");
    }

    /**
     * @Author 70312
     * @Description 定位文件读取（下载第一块）
     * @Date 8:57 2021/8/20
     * @Param * @param null
     * @return
     */
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("f:/hadoop-2.7.2.tar.gz.part1"));

        // 流的拷贝
        byte[] buf = new byte[1024];
        for (int i = 0;i < 1024*128;i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

        System.out.println("over");
    }

    /**
     * @Author 70312
     * @Description 定位文件读取（下载第二块
     * @Date 11:20 2021/8/20
     * @Param * @param null
     * @return
     */
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 定位输入数据位置
        fis.seek(1024*1024*128);

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("f:/hadoop-2.7.2.tar.gz.part2"));

        // 流的对拷
        IOUtils.copyBytes(fis,fos,configuration);

        // 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        System.out.println("over");
    }
}