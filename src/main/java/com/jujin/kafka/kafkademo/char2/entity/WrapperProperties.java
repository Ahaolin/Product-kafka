package com.jujin.kafka.kafkademo.char2.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Properties;

/**
 * 包装Config对象
 * @author MSI
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WrapperProperties<T> {
    private Properties props;
    private T t;
}
