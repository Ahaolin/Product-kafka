package com.jujin.kafka.kafkademo.concurrent.aaa;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class ThreadSafeSample {
    private static final Unsafe U;
    private static final long SIZECTL;
    public volatile int sharedState;

    static {
        try {
//            U = sun.misc.Unsafe.getUnsafe();
            U =  getUnsafe();
            SIZECTL = U.objectFieldOffset(ThreadSafeSample.class.getDeclaredField("sharedState"));
            System.out.println(SIZECTL);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new Error(e);
        }
    }

    static private sun.misc.Unsafe getUnsafe() throws IllegalArgumentException, IllegalAccessException {
        Class<?> cls = sun.misc.Unsafe.class;
        Field[] fields = cls.getDeclaredFields();
        for(Field f : fields) {
            if("theUnsafe".equals(f.getName())) {
                f.setAccessible(true);
                return (sun.misc.Unsafe) f.get(null);
            }
        }
        throw new IllegalAccessException("no declared field: theUnsafe");
    }


    public void nonSafeAction() {
        while (sharedState < 1000) {
            if (U.compareAndSwapInt(this,SIZECTL,sharedState,sharedState+1)){
                System.out.println(Thread.currentThread().getName() + "  " + sharedState);
            }
//            int former = sharedState++;
//            int latter = sharedState;
//            if (former != latter - 1) {
//                System.out.printf("Observed data race, former is " + former + ", " + "latter is " + latter);
//            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ThreadSafeSample sample = new ThreadSafeSample();
        Thread threadA = new Thread("A") {
            public void run() {
                sample.nonSafeAction();
            }
        };
        Thread threadB = new Thread("B") {
            public void run() {
                sample.nonSafeAction();
            }
        };
        Thread threadC = new Thread("C") {
            public void run() {
                sample.nonSafeAction();
            }
        };
        threadA.start();
        threadB.start();
        threadC.start();
        threadA.join();
        threadB.join();
        threadC.join();
    }
}