package com.jujin.kafka.kafkademo.concurrent.aaa;

import java.util.concurrent.atomic.AtomicStampedReference;
public class AtomicStampedReferenceTest {
    static AtomicStampedReference<Integer> stampedReference = new AtomicStampedReference(0, 0);
    public static void main(String[] args) throws InterruptedException {
        final int stamp = stampedReference.getStamp();
        final Integer reference = stampedReference.getReference();
        System.out.println(reference+"============"+stamp);
        Thread t1 = new Thread(() -> System.out.println(reference + "-" + stamp + "-" + stampedReference.compareAndSet(reference, reference + 10, stamp, stamp + 1)));

        Thread t2 = new Thread(() -> {
                Integer reference1 = stampedReference.getReference();
                System.out.println(reference1 + "-" + stamp + "-"
                + stampedReference.compareAndSet(reference1, reference1 + 10, stamp, stamp + 1));
        });
        t1.start();
        t1.join();
        t2.start();
        t2.join();

        System.out.println(stampedReference.getReference());
        System.out.println(stampedReference.getStamp());
    }
}