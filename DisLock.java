package com.kk.search.as;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: shh
 * @create: 2018-08-23 12-09
 */
public class DisLock {

    private static final String ROOT_PATH = "lock";
    private static final String SUB_PATH = "/sub";
    private static final int DEFAULT_CONNECT_TIMEOUT = 1000;
    private static final int DEFAULT_SESSION_TIMEOUT = 1000;
    private static final int DEFAULT_LOCK_TIMEOUT = 120;
    private CuratorFramework client;


    private DisLock() {
        client = CuratorFrameworkFactory.builder().connectString("10.9.17.46:2181,10.9.18.29:2181,10.9.26.172:2181")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectionTimeoutMs(DEFAULT_CONNECT_TIMEOUT)
                .sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT).namespace(ROOT_PATH).build();
        client.start();
    }

    private CuratorFramework getClient() {
        return client;
    }
    public void close() {
        CloseableUtils.closeQuietly(client);
    }
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                DisLock dk = new DisLock();
                InterProcessMutex lock = new InterProcessMutex(dk.getClient(), SUB_PATH);
                try {
                    lock.acquire(DEFAULT_LOCK_TIMEOUT, TimeUnit.SECONDS);
                    System.out.println("当前线程：" + Thread.currentThread().getName() + "开始执行任务。。");
                    Thread.sleep(20000);
                    System.out.println("当前线程：" + Thread.currentThread().getName() + "任务完成");
                    lock.release();
                    dk.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}
