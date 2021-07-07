package com.dp.elasticsearch.service.impl;

import com.alibaba.fastjson.JSON;
import com.dp.elasticsearch.entity.EsEntity;
import com.dp.elasticsearch.service.IEsRestService;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author songqinglong
 */
@Service
public class EsRestServiceImpl implements IEsRestService {

    private final static Logger log = LoggerFactory.getLogger(EsRestServiceImpl.class);

    @Qualifier("highLevelClient")
    @Autowired
    private RestHighLevelClient client;

    @Override
    public String add(XContentBuilder content, String index, String id) {
        String Id = null;
        try {
            IndexRequest request = new IndexRequest(index).id(id).source(content);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            Id = response.getId();
//            log.info("索引:{},数据添加,返回码:{},id:{}", index, response.status().getStatus(), Id);
        } catch (Exception e) {
            log.error("添加数据失败,index:{},id:{}", index, id);
        }
        return Id;
    }

    @Override
    public String update(XContentBuilder content, String index, String id) {
        String Id = null;
        try {
            UpdateRequest request = new UpdateRequest(index, id).doc(content);
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            Id = response.getId();
            log.info("数据更新,返回码:{},id:{}", response.status().getStatus(), Id);
        } catch (Exception e) {
            log.error("数据更新失败,index:{},id:{}", index, id);
        }
        return Id;
    }

    @Override
    public String insertBatch(String index, List<EsEntity> list) {
        String state = null;
        BulkRequest request = new BulkRequest();
        list.forEach(item -> request.add(new IndexRequest(index)
                .id(item.getId()).source(JSON.toJSONString(item.getData()), XContentType.JSON)));
        try {
            BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
            int status = bulk.status().getStatus();
            state = Integer.toString(status);
            log.info("索引:{},批量插入{}条数据成功!", index, list.size());
        } catch (Exception e) {
            log.error("索引:{},批量插入数据失败", index);
            log.error(e.getMessage(),e);
        }
        return state;
    }

    @Override
    public void deleteByQuery(String index, QueryBuilder builder) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(builder);
        //设置此次删除的最大条数
        request.setBatchSize(1000);
        long deleted = 1;
        while (deleted > 0) {
            try {
                deleted = client.deleteByQuery(request, RequestOptions.DEFAULT).getDeleted();
                log.info("根据条件删除数据,index:{},num:{}", index,deleted);
            } catch (Exception e) {
                deleted = 1;
                log.error("根据条件删除数据失败,index:{}", index);
            }
        }
    }

    @Override
    public String deleteById(String index, String id) {
        String state = null;
        DeleteRequest request = new DeleteRequest(index, id);
        try {
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            int status = response.status().getStatus();
            state = Integer.toString(status);
            log.info("索引:{},根据id{}删除数据:{}", index, id, JSON.toJSONString(response));
        } catch (Exception e) {
            log.error("根据id删除数据失败,index:{},id:{}", index, id);
        }
        return state;
    }

    @Override
    public List searchDatePage(String index, int startPage, int pageSize, SearchSourceBuilder sourceBuilder, QueryBuilder queryBuilder) {
        SearchRequest request = new SearchRequest(index);
        //设置超时时间
        sourceBuilder.timeout(new TimeValue(120, TimeUnit.SECONDS));
        //设置是否按匹配度排序
        sourceBuilder.explain(true);
        //加载查询条件
        sourceBuilder.query(queryBuilder);
        //设置分页
        sourceBuilder.from((startPage - 1) * pageSize).size(pageSize);
        log.info("查询返回条件：" + sourceBuilder.toString());
        request.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            long totalHits = searchResponse.getHits().getTotalHits().value;
            log.info("共查出{}条记录", totalHits);
            RestStatus status = searchResponse.status();
            if (status.getStatus() == 200) {
                List<Map<String, Object>> sourceList = new ArrayList<>();
                for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                    Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
                    sourceList.add(sourceAsMap);
                }
                return sourceList;
            }
        } catch (Exception e) {
            log.error("条件查询索引{}时出错", index);
        }
        return null;
    }
}
