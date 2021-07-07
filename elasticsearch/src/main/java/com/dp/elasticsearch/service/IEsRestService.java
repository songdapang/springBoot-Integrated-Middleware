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
     * @param content 数据内容
     * @param index   索引
     * @param id      id
     */
    public String add(XContentBuilder content, String index, String id);
    /**
     * 修改数据
     *
     * @param content 修改内容
     * @param index   索引
     * @param id      id
     */
    public String update(XContentBuilder content, String index, String id);
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
     * @param builder 删除条件
     */
    public void deleteByQuery(String index, QueryBuilder builder);
    /**
     * 根据id删除数据
     *
     * @param index 索引
     * @param id    id
     */
    public String deleteById(String index, String id);
    /**
     * 根据条件查询数据
     *
     * @param index         索引
     * @param startPage     开始页
     * @param pageSize      每页条数
     * @param sourceBuilder 查询返回条件
     * @param queryBuilder  查询条件
     */
    public List searchDatePage(String index, int startPage, int pageSize,
                               SearchSourceBuilder sourceBuilder, QueryBuilder queryBuilder);
}
