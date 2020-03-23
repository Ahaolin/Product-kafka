package com.jujin.kafka.kafkademo.text;



import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * java复习的text包
 */
public class JavaTestDemo {

    @Test
    public void textMap(){
        Map<String,Object> map = new HashMap<>();
        map.put("adsad","");

    }


    @Test
    public void textAlg(){
        int a[] = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int temp [] = a;
        int count = 1;
        while(true){
            if (temp.length <= 1 || count > 10){
                break;
            }
            temp = new int[10];
            for (int i = 0,j = -1; i < a.length; i+=2,j++) {
                if (i==0 && a[i] == 0){
                    continue;
                }
                temp[j] = a[i];
            }
            count ++;
            System.out.println(String.format("第%d次循环：%s" ,count, getDateStr(temp)));

            a = temp;
        }
        System.out.println(getDateStr(temp));
    }


    public static String getDateStr(int [] a) {
        StringBuffer sbf = new StringBuffer("[");
        for (int i = 0; i < a.length; i++) {
            sbf.append(a[i]).append(",");
        }
        sbf.append("]");
        return sbf.toString();
    }
}
