package com.jujin.kafka.kafkademo.concurrent.juc;

import cn.hutool.core.util.StrUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TicketLock {
    /**
     * 服务号
     */
    private AtomicInteger serviceNum = new AtomicInteger();
    /**
     * 排队号
     */
    private AtomicInteger ticketNum = new AtomicInteger();

    /**
     * lock:获取锁，如果获取成功，返回当前线程的排队号，获取排队号用于释放锁. <br/>
     *
     * @return
     */
    public int lock() {
        int currentTicketNum = ticketNum.incrementAndGet();
        System.out.println(StrUtil.format("当前线程：{} 开始加锁：currentTicketNum：{} serviceNum：{}", Thread.currentThread().getName(), currentTicketNum, serviceNum.get()));
        while (currentTicketNum != serviceNum.get()) {
            // Do nothing
            System.out.println(StrUtil.format("当前线程：{} 计算循环：currentTicketNum：{} serviceNum：{}", Thread.currentThread().getName(), currentTicketNum, serviceNum.get()));
        }
        return currentTicketNum;
    }

    /**
     * unlock:释放锁，传入当前持有锁的线程的排队号 <br/>
     *
     * @param ticketnum
     */
    public void unlock(int ticketnum) {
        System.out.println(StrUtil.format("当前线程：{} 开始解锁：ticketnum：【{}】 serviceNum：【{}】 ticketNum：【{}】", Thread.currentThread().getName(), ticketnum, serviceNum.get(), ticketNum.get()));
        serviceNum.compareAndSet(ticketnum, ticketnum + 1);
    }


    /**
     * 测试 该乐观锁  是否正确
     * 错误原因：两个线程
     *
     * @param args
     */
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        TicketLock lock = new TicketLock();
        executorService.execute(() -> {
            Thread.currentThread().setName("线程1");
            int ticketnum = lock.lock();
            lock.unlock(ticketnum);
        });

        executorService.execute(() -> {
            Thread.currentThread().setName("线程2");
            int ticketnum = lock.lock();
            lock.unlock(ticketnum);
        });
        executorService.shutdown();
    }


}