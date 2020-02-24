package com.jujin.kafka.kafkademo.char2.consumer.deserializer;

import com.jujin.kafka.kafkademo.char2.entity.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author MSI
 * 自定义解码器
 * {@link com.jujin.kafka.kafkademo.char2.producer.serializer.CompanySerializer} 编码器
 *  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,CompanyDeserializer.class.getName());
 */
public class CompanyDeserializer implements Deserializer<Company> {

    /**
     *配置当前类
     *
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    /**
     * 执行反序列化。如果 data 为 null，那么处理的时候直接返回 null 而不是抛出一个异常。
     */
    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new SerializationException("Size of data received " +
                    "by DemoDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error occur when deserializing!");
        }

        return new Company(name,address);
    }

    /**
     * 关闭当前序列化器
     */
    @Override
    public void close() {}
}
