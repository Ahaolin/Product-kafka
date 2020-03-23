package com.jujin.kafka.kafkademo.concurrent.aaa;

import org.junit.Test;

import java.util.concurrent.*;

/**
 * 测试 线程池
 */
public class textExecutors {

    @Test
    public void textCached(){
        ExecutorService executorService = Executors.newCachedThreadPool();
        new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
        /**
         *  SynchronousQueue
         */
    }

    @Test
    public void textFixed(){
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        new ThreadPoolExecutor(3, 3,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(3));
        /**
         *  LinkedBlockingQueue 默认是无界队列  可以手动指定大小
         */
    }


    @Test
    public void textSingle(){
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(3));
        /**
         *  LinkedBlockingQueue 默认是无界队列  可以手动指定大小
         */
    }


    @Test
    public void textSingleThreadScheduled(){
        ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ExecutorService executorService1 = Executors.newScheduledThreadPool(12);
    }


    @Test
    public void textClass()  {

    }


    public static void main(String[] args) throws ClassNotFoundException{
        Class.forName("com.jujin.kafka.kafkademo.concurrent.aaa.textExecutors")  ;
    }

}
