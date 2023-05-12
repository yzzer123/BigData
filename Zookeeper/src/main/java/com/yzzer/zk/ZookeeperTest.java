package com.yzzer.zk;

import com.sun.jmx.remote.internal.ArrayQueue;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.swing.plaf.nimbus.State;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 1. 获取ZK连接
 * 2. 调用API
 * 3. 关闭资源
 */

public class ZookeeperTest {
    private ZooKeeper zooKeeper;

    @Before
    public void init() throws IOException {
         zooKeeper = new ZooKeeper("master:2181, slave01:2181", 10000, new Watcher() {
             @Override
             public void process(WatchedEvent event) {

//                 System.out.println("连接状态：" + event.getState());
             }
         });

    }

    /**
     * 创建节点测试
     * 1. path 创建路径
     * 2. content 内容
     * 3. Acl 权限设置 ZooDefs.Ids枚举
     * 4. CreateMode 创建模式 持久化，临时的
     */
    @Test
    public void createTest() throws InterruptedException, KeeperException {
        zooKeeper.create("/yzzertest", "haha".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    /**
     * 获取子节点
     */
    @Test
    public void getChildTest() throws InterruptedException, KeeperException {
        // 不监控的方式
//        List<String> children = zooKeeper.getChildren("/", false);
        List<String> children = zooKeeper.getChildren("/", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("发现该目录下变化");
                System.exit(0);
            }
        });


        for (String child : children) {
            System.out.println(child);
        }


        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 测试节点存在
     */
    public Stat exitsTest(String str) throws InterruptedException, KeeperException {
        // 不监控的方式
        Stat state = zooKeeper.exists(str, false);
//        System.out.println(state);
        if(state == null){
            System.out.println("不存在");
        }else{
            System.out.println("存在");
        }
        return state;
    }

    /**
     * 获取子节点数据
     */
    @Test
    public void getDataTest() throws InterruptedException, KeeperException {
        // 不监控的方式
        byte[] data = zooKeeper.getData("/yzzertest", false, exitsTest("/yzzertest"));

        System.out.println(new String(data));

    }

    /**
     * 设置子节点数据
     */
    @Test
    public void setDataTest() throws InterruptedException, KeeperException {
        zooKeeper.setData("/yzzertest", "my name is yzzer".getBytes(),exitsTest("/yzzertest").getVersion());

    }

    /**
     * 删除子节点
     */
    @Test
    public void deleteTest() throws InterruptedException, KeeperException {
        zooKeeper.delete("/honglou", exitsTest("/honglou").getVersion());
    }


    /**
     * 删除有子节点的节点
     */
    @Test
    public void deleteAllTest() throws InterruptedException, KeeperException {
        String p = "/yzzertest";
        Stack<String> stack = new Stack<>();
        stack.push(p);

        while(!stack.isEmpty() && p != null){
            p = stack.peek();
            while(exitsTest(p).getNumChildren()>0){
                p = p + "/" + zooKeeper.getChildren(p, false).get(0);
                stack.push(p);
            }
            System.out.println(p);
            zooKeeper.delete(p, -1);
            stack.pop();
        }


    }

    @Test
    public void Test() throws InterruptedException, KeeperException {
        System.out.println(exitsTest("/yzzertest"));
    }

    @After
    public void close() throws InterruptedException {

        zooKeeper.close();
    }

}
