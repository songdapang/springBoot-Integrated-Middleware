package com.dp.elasticsearch.entity;

import lombok.Data;

/**
 * @Author songqinglong
 * @Date 2021/7/10 6:44 下午
 * @Description: 测试用的demo实体类 查询的时候字段为null时不进行匹配该字段
 * @Version 1.0
 */
@Data
public class EsDemoEntity {


    private String name;

    private String age;

    private String phone;

}
