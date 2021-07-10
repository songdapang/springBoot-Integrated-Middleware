package com.dp.elasticsearch.entity;

import lombok.Data;
/***
* @Description: es批量存储时存储对象
* @Param:
* @return:
* @Author: songqinglong
* @Date: 2021/7/10
*/

@Data
public class EsEntity {

    /** 唯一键 */
    private String id;

    /** 具体要存储对象 */
    private Object data;

}

