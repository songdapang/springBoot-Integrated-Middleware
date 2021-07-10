package com.dp.elasticsearch.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author songqinglong
 * @Date 2021/7/10 6:35 下午
 * @Description:
 * @Version 1.0
 */
public class BeanUtils extends org.springframework.beans.BeanUtils {

    /***
     * @Description: 获取一个对象的字段名 字段对应的值
     * @Param: [data] 实体对象
     * @return: Map<String,Object>
     * @Author: songqinglong
     * @Date: 2021/7/9
     */
    public static Map<String,Object> parseClass(Object data) {

        Class<?> clazz = data.getClass();
        Field[] fields = clazz.getDeclaredFields();
//        String[] names = new String[fields.length];     // 所有的字段名
//        Object[] values = new Object[fields.length];    // 所有的属性值
        Map<String,Object> result = new HashMap<>();
        try{
            Field.setAccessible(fields,true);
            for(int i=0;i<fields.length;i++) {
                result.put(fields[i].getName(),fields[i].get(data));
            }
            return result;
        } catch (Exception e) {
            return null;
        }

    }

}
