package com.jujin.kafka.kafkademo.text;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.stream.Stream;

public class textString {

    public static void main(String[] args) {
//        String str1 = "abcedfghhhhh";
//        String str2 = new String("abcedfghhhhh");
//        System.out.println(str1 == str2);
//
//        String str3 = str2.intern();
//        System.out.println(str1 == str2);
//        System.out.println(str1 == str3);

//        TreeMap<DefaultA,String> map = new TreeMap<>();
//        map.put(new DefaultA(),"123");
//        Object[] objects = Stream.of(map).toArray();
//        System.out.println(Arrays.toString(objects));


//        TreeMap<Integer, String> map = new TreeMap<>();
//        map.put(10, "10");
//        map.put(85, "85");
//        map.put(15, "15");
//        System.out.println(map);

        HashMap<Integer, String> data = new HashMap<>();
//        data.put(10, "10");
//        data.put(11, "85");
//        data.put(12, "15");
//        data.put(13, "15");
//        data.put(14, "15");
//        data.put(15, "15");
//        data.put(16, "15");
//        data.put(17, "15");
//        data.put(18, "15");
//        data.put(18, "15");
//        System.out.println(data);


//        List<Integer> list = new CopyOnWriteArrayList<Integer>();
//        list.add(1);
//        list.add(2);

//        int hash = hash("123");
//        for (int i = 2; i < 100; i*=2) {
//            System.out.println(i + " : " + ((i - 1) & hash) + "[" + hash%i + "]");
//        }

        ConcurrentHashMap<String,Object> map = new ConcurrentHashMap();
        map.put("","");
        int n = 16;
        System.out.println( n - (n >>> 2));

        /**
         * sizeCtl
         * volatile int sizeCtl;
         * 该属性用来控制 table 数组的大小，根据是否初始化和是否正在扩容有几种情况：
         * **当值为负数时：**如果为-1 表示正在初始化，如果为-N 则表示当前正有 N-1 个线程进行扩容操作；
         * **当值为正数时：**如果当前数组为 null 的话表示 table 在初始化过程中，sizeCtl 表示为需要新建数组的长度；
         * 若已经初始化了，表示当前数据容器（table 数组）可用容量也可以理解成临界值（插入节点数超过了该临界值就需要扩容），具体指为数组的长度 n 乘以 加载因子 loadFactor；
         * 当值为 0 时，即数组长度为默认初始值。
         *
         */


//        java.lang.Runtime


        // TODO Auto-generated method stub
        int[] a = new int[10];
        a[0] = 0;
        a[1] = 1;
        a[2] = 2;
        a[3] = 3;
        System.arraycopy(a, 2, a, 3, 3);
//        a[2]=99;
        for (int i = 0; i < a.length; i++) {
            System.out.println(a[i]);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(()-> System.out.println("sda"));

    }

    static final int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    static class DefaultA implements Comparable {
        private String name;
        private String msg;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

    }
}


