package com.jujin.kafka.kafkademo.concurrent.aaa;

import lombok.SneakyThrows;

import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @date 2020年03月24日 17:52:43
 * @author MSI
 * 学习  CountDownLatch 和 CyclicBarrier
 *  使用对账做例子  订单和派送单  需要将差异保存至差异库中
 *
 * CyclicBarrier 和 CountDownLatch 的区别？
 *  CountDownLatch 是计数器，只能使用一次。(一个线程（多个线程）等待,而其他的 N 个线程在完成“某件事情”之后，可以终止，也可以等待。)
 *  CyclicBarrier 的计数器提供 reset 功能，可以多次使用。（重点是多个线程，在任意一个线程没有完成，所有的线程都必须等待）
 */
public abstract class CountDownLatchAndCyclicBarrierExample {


    abstract class PDodders {
        protected Object pos;
        protected Object dos;
        protected Object diff;

        /**
         * 比较对账的差异 并且保存进数据库
         */
        @SneakyThrows
        public void checkAndSaveChange() {
            while (hasPOrders()) {
                // 查询未对账订单
                Thread T1 = new Thread(() -> {
                    pos = getPOrders();
                });
                T1.start();
                // 查询派送单
                Thread T2 = new Thread(() -> {
                    dos = getDOrders();
                });
                T2.start();
                // 等待T1、T2结束
                T1.join();
                T2.join();
                // 执行对账操作
                diff = check(pos, dos);
                // 差异写入差异库
                save(diff);
            }
        }

        /**
         * 保存差异
         */
        protected abstract void save(Object diff);

        /**
         * 返回订单和派送单的结果
         */
        protected abstract Object check(Object pos, Object dos);

        /**
         * 存在未对账订单
         */
        protected abstract boolean hasPOrders();

        /**
         * 获取订单
         */
        protected abstract Object getPOrders();

        /**
         * 获取订单
         */
        protected abstract Object getDOrders();
    }

    /**
     * 使用CountDownLatch进行优化 {@link CyclicBarrierPDodders}
     * 将获取 订单和派送单并行了  还可以继续优化 两次查询(生产者) -》 对账操作（消费者）
     */
    abstract class CountDownLatchPDodders extends PDodders {

        // 创建2个线程的线程池
        Executor executor = Executors.newFixedThreadPool(2);

        @SneakyThrows
        @Override
        public void checkAndSaveChange() {
            while (hasPOrders()) {
                // 计数器初始化为2
                CountDownLatch latch = new CountDownLatch(2);
                // 查询未对账订单
                executor.execute(() -> {
                    pos = getPOrders();
                    latch.countDown();
                });
                // 查询派送单
                executor.execute(() -> {
                    dos = getDOrders();
                    latch.countDown();
                });

                // 等待两个查询操作结束
                latch.await();
                // 执行对账操作
                diff = check(pos, dos);
                // 差异写入差异库
                save(diff);
            }
        }
    }

    /**
     * 比起{@link CountDownLatchPDodders}  添加队列保存生产的信息，消费者从队列中进行消费消息
     *
     * 两个线程不停将 订单数据 和 运单数据 保存在队列中
     *  每查询到一条 调用awit()方法 如果CyclicBarrier为0 则调用如果CyclicBarrier为0回调方法 并将重新设置 【值、线程】
     */
    abstract class CyclicBarrierPDodders extends PDodders {
        protected Vector<Object> pos; // 订单队列
        protected Vector<Object> dos; // 派送单队列

        // 执行回调的线程池
        Executor executor = Executors.newFixedThreadPool(1);
        final CyclicBarrier barrier = new CyclicBarrier(2, () -> executor.execute(() -> {
            // 回调函数
            Object p = pos.remove(0);
            Object d = dos.remove(0);
            // 执行对账操作
            diff = check(p, d);
            // 差异写入差异库
            save(diff);
        }));

        /**
         * CyclicBarrier 的回调函数我们使用了一个固定大小的线程池？
         *  1.使用线程池是为了异步操作，否则CyclicBarrier的回掉函数是同步调用的，也就是本次对账操作执行完才能进行下一轮的检查。
         *  2.线程数量固定为1，防止了多线程并发导致的数据不一致，因为订单和派送单是两个队列，只有单线程去两个队列中取消息才不会出现消息不匹配的问题。
         *
         *  CyclicBarrier的回调函数执行在一个回合里最后执行await()的线程上，
         *  而且同步调用回调函数，调用完之后，才会开始第二回合。所以必须使用另外开一个线程执行回调。
         */
        @Override
        @SneakyThrows
        public void checkAndSaveChange() {
            // 循环查询订单库
            new Thread(() -> {
                while (hasPOrders()) {
                    // 查询订单库
                    pos.add(getPOrders());
                    // 等待
                    barrier.await();
                }
            },"Thread-订单查询").start();
            // 循环查询运单库
            new Thread(() -> {
                while (hasPOrders()) {
                    // 查询运单库
                    dos.add(getDOrders());
                    // 等待
                    barrier.await();
                }
            },"Thread-运单查询").start();
        }
    }

}
