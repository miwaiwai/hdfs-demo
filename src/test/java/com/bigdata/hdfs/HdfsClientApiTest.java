package com.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 通过API操作HDFS测试类
 */
public class HdfsClientApiTest {

    @Test
    public void initHDFS() throws IOException {
        // 创建配置信息对象
        Configuration configuration = new Configuration();
        // 获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        // 打印文件系统
        System.out.println(fs.toString());
    }

    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        // 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 上传文件
        fs.copyFromLocalFile(new Path("f:/hello.txt"),new Path("/hello3.txt"));

        // 关闭资源
        fs.close();

        System.out.println("over");
    }

    /**
     * 功能描述:
     * 〈HDFS文件下载〉
     */
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");
        // 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件效验
        fs.copyToLocalFile(false,new Path("/hello2.txt"),new Path("f:/hello2.txt"),true);

        // 关闭资源
        fs.close();

        System.out.println("over");
    }
    
    /**
     * 功能描述:
     * 〈HDFS目录创建〉
     */
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 创建目录
        fs.mkdirs(new Path("/0819/myy/zling"));

        // 关闭资源
        fs.close();

        System.out.println("over");
    }

    /**
     * 功能描述: <br>
     * 〈HDFS文件夹删除〉
     */
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 执行删除
        fs.delete(new Path("/0819/"),true);

        fs.close();

        System.out.println("over");
    }

    /**
     * 功能描述:
     * 〈HDFS文件名更改〉
     */
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 修改文件名称
        fs.rename(new Path("/hello2.txt"),new Path("/hello6.txt"));

        fs.close();
        System.out.println("over");
    }

    /**
     * 功能描述: <br>
     * 〈HDFS文件详情查看〉
     */
    @Test
    public void testListFile() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");
        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // z组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("----------------分割线----------------");
        }
    }

    /**
     * 功能描述: <br>
     * 〈HDFS文件和文件夹判断〉
     */
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        // 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop100:9000"),configuration,"bigdata");

        // 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if(fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 关闭资源
        fs.close();
        System.out.println("over");
    }

    /**
     * 功能描述: <br>
     * 〈〉
     *
     */
}