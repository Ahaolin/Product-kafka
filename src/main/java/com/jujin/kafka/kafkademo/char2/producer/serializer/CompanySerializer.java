package com.jujin.kafka.kafkademo.char2.producer.serializer;

import com.jujin.kafka.kafkademo.char2.entity.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author MSI
 * //代码清单4-2 自定义的序列化器CompanySerializer
 * 必须实现Serializer接口
 */
public class CompanySerializer implements Serializer<Company> {
    /**
     * 配置当前类
     * 在创建 KafkaProducer 实例的时候调用的，主要用来确定编码类型
     * @param configs
     * @param isKey
     */
    @Override
    public void configure(Map configs, boolean isKey) {}

    /**
     * 执行序列化操作
     */
    @Override
    public byte[] serialize(String topic, Company data) {
        if (data == null) {
            return null;
        }
        byte[] name, address;
        try {
            if (data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.
                    allocate(4+4+name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    /**
     * 关闭当前的序列化器
     * 一般情况下 close() 是一个空方法，如果实现了此方法，则必须确保此方法的幂等性，因为这个方法很可能会被 KafkaProducer 调用多次。
     */
    @Override
    public void close() {}
}
