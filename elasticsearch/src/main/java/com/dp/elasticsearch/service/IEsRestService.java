package com.dp.elasticsearch.service;

import com.dp.elasticsearch.entity.EsEntity;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

public interface IEsRestService {
    /**
     * 添加数据
     *
     * @param data 数据内容 传对象就行
     * @param index   索引
     * @param id      id
     */
    public String add(Object data, String index, String id);
    /**
     * 修改数据
     *
     * @param data 修改内容 传对象就行
     * @param index   索引
     * @param id      id
     */
    public String update(Object data, String index, String id);
    /**
     * 批量插入数据
     *
     * @param index 索引
     * @param list  批量增加的数据
     */
    public String insertBatch(String index, List<EsEntity> list);
    /**
     * 根据条件删除数据
     *
     * @param index   索引
     * @param data 删除条件
     */
    public void deleteByQuery(String index, Object data);
    /**
     * 根据id删除数据
     *
     * @param index 索引
     * @param id    id
     */
    public String deleteById(String index, String id);

    /***
     * @Description: 判断索引是否存在
     * @Param: [indexName] 索引
     * @return: boolean
     * @Author: songqinglong
     * @Date: 2021/7/8
     */

    public boolean exists(String indexName);

    /**
     * 删除索引
     * @param index 索引
     */
    public Boolean deleteByIndex(String index);

    /**
     * 根据条件查询数据 （1w条上限 即如果该index下数据量超过1w条 超过部分查不到）
     * startPage =0 || pageSize = 0 不分页 全部查出来
     * @param index         索引
     * @param startPage     开始页
     * @param pageSize      每页条数
     * @param data          查询的对象
     */
    public List searchDatePage(String index, int startPage, int pageSize,Object data);

    /**
     * 批量插入数据 异步
     * @param index 索引
     * @param list  批量增加的数据
     */
    public String insertBatchAsyn(String index, List<EsEntity> list);

    /***
     * @Description: 查询index下所有的数据
     * @Param: [index] es的索引
     * @Param: [data] 查询bean
     * @return: java.lang.String
     * @Author: songqinglong
     * @Date: 2021/7/9
     */
     public List getAllByIndex(String index, Object data);
}
