package com.yzzer.hdfs;

/*
* 1. 和HDFS建立连接
* 2. 调用API完成具体功能
* 3. 关闭连接
* */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;

public class HdfsClientTest {

    private FileSystem fs;

    /**
     * 获取FileSystem对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws IOException, InterruptedException{
        URI uri = URI.create("hdfs://hadoopcluster");


        // 配置对象conf
        Configuration conf = new Configuration();
//        conf.set("dfs.replication", "3");
        conf.set("fs.defaultFS", "hdfs://hadoopcluster");
        conf.set("dfs.nameservices", "hadoopcluster");
        conf.set("dfs.ha.namenodes.hadoopcluster", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.hadoopcluster.nn1", "hadoop102:8020");
        conf.set("dfs.namenode.rpc-address.hadoopcluster.nn2", "hadoop103:8020");
        //conf.setBoolean(name, value);
        conf.set("dfs.client.failover.proxy.provider.hadoopcluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // 指定操作用户
        String user = "atguigu";

        // 获取HDFS客户端连接对象
        FileSystem fileSystem = FileSystem.get(uri, conf, user);


        fs = fileSystem;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 查看文件详情
     * @throws IOException
     */
    @Test
    public void testListFiles() throws IOException{
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件名：" + fileStatus.getPath().getName());
            System.out.println("块大小" + fileStatus.getBlockSize());
            System.out.println("副本数" + fileStatus.getReplication());
            System.out.println("权限" + fileStatus.getPermission());
            System.out.println();
        }
    }

    /**
     * 拷贝文件到HDFS
     * 配置文件的优先级  Configuration > hdfs-site.xml > hdfs-default.xml
     * @throws IOException
     */
    @Test
    public void testCopyFromLocal() throws IOException {
        fs.copyFromLocalFile(false, true,
                new Path("/Volumes/yzzerport/hadoop/03_测试数据/phone_data/phone_data.txt"),
                new Path("/client_test/test2.txt"));
    }

    /**
     * 文件的更名和移动
     * @throws IOException
     */
    @Test
    public void testRename() throws IOException{
//        fs.rename(new Path("/client_test/test2.txt"), new Path("/wcinput")); // 复制
          fs.rename(new Path("/client_test/test.txt"), new Path("/client_test/test2.txt"));  //改名
    }



    /**
     * 判断文件是否是目录
     * @throws IOException
     */
    @Test
    public void testListStatus() throws IOException{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus status: fileStatuses){
            if(status.isDirectory()){
                System.out.println("DIR:" + status.getPath().getName());
            }else{
                System.out.println("FILE:" + status.getPath().getName());
            }
        }
    }


    /**
     * 删除文件和目录
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException{
        fs.delete(new Path("/wcoutput"),true);
    }

    /**
     * 下载文件
     * // @param delSrc
     *     whether to delete the src
     *  //@param src
     *      path
     *  //@param dst
     *      path
     * // @param useRawLocalFileSystem
     *       whether to use RawLocalFileSystem as local file system or not.
     * @throws IOException
     */
    @Test
    public void testCopyToLocal() throws IOException{
        fs.copyToLocalFile(false, new Path("/client_test/test2.txt")
        ,new Path("/Users/yzzer/lib/data.txt"), true);
    }


    /*
    * 获取HDFS客户端的连接对象
    * @param uri of the filesystem hdfs://192.168.1.88:9820
     * @param conf the configuration to use  配置对象
     * @param user to perform the get as    操作用户
    * */
    @Test
    public void testCreateHdfsClient() throws IOException, InterruptedException {
        // HDFS的访问路径
        URI uri = URI.create("hdfs://labserver:9820");

        // 配置对象conf
        Configuration conf = new Configuration();

        // 指定操作用户
        String user = "yzzer";

        // 获取HDFS客户端连接对象
        FileSystem fileSystem = FileSystem.get(uri, conf, user);
        System.out.println(fileSystem.getClass().getName());

        // 关闭资源
        fileSystem.close();
    }
}
