package com.jujin.kafka.kafkademo.concurrent.aaa;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 查看可重入锁和条件 源码
 */
public class TestRerunLockCondition {

    final Lock lock = new ReentrantLock();
    // 队列不满（队列已满不允许入队）
    final Condition notFull = lock.newCondition();
    // 队列不空（空队列不允许出队）
    final Condition notEmpty = lock.newCondition();

    final Object[] items = new Object[100];
    int putptr, takeptr, count;

    // 生产
    public void put(Object x) throws InterruptedException {
        lock.lock();
        try {
            while (count == items.length)
                notFull.await();  // 队列已满，等待，直到 not full 才能继续生产
            items[putptr] = x;
            if (++putptr == items.length) putptr = 0;
            ++count;
            notEmpty.signal(); // 生产成功，队列已经 not empty 了，发个通知出去
        } finally {
            lock.unlock();
        }
    }

    // 消费
    public Object take() throws InterruptedException {
        lock.lock();
        try {
            while (count == 0)
                notEmpty.await(); // 队列为空，等待，直到队列 not empty，才能继续消费
            Object x = items[takeptr];
            if (++takeptr == items.length) takeptr = 0;
            --count;
            notFull.signal(); // 被我消费掉一个，队列 not full 了，发个通知出去
            return x;
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) {
        Object o = new Object();
        synchronized (TestRerunLockCondition.class){
            try {
                o.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }catch (Exception e){
                System.out.println( "sdsd1 :" +Thread.currentThread().isInterrupted());
            }
            Thread.currentThread().interrupt();
            System.out.println( "sdsd :" +Thread.currentThread().isInterrupted());
        }
        System.out.println( "sdsd2 :" +Thread.currentThread().isInterrupted());

    }

}
