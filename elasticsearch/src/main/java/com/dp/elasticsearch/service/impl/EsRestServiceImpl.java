package com.dp.elasticsearch.service.impl;

import com.alibaba.fastjson.JSON;
import com.dp.elasticsearch.entity.EsEntity;
import com.dp.elasticsearch.service.IEsRestService;
import com.dp.elasticsearch.utils.BeanUtils;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    public String add(Object data, String index, String id) {
        String Id = null;
        try {

            XContentBuilder builder = wrapperXcontentBuilder(data);

            IndexRequest request = new IndexRequest(index).id(id).source(builder);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            Id = response.getId();
//            log.info("索引:{},数据添加,返回码:{},id:{}", index, response.status().getStatus(), Id);
        } catch (Exception e) {
            log.error("添加数据失败,index:{},id:{}", index, id);
        }
        return Id;
    }

    private XContentBuilder wrapperXcontentBuilder(Object data) throws Exception {
        Map<String, Object> dataMap = BeanUtils.parseClass(data);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        dataMap.forEach((k,v)->{
            try {
                builder.field(k,v==null?"":v);
            } catch (Exception e) {
                log.error(e.getMessage(),e);
            }
        });
        builder.endObject();
        return builder;
    }

    @Override
    public String update(Object data, String index, String id) {
        String Id = null;
        try {
            XContentBuilder builder = wrapperXcontentBuilder(data);
            UpdateRequest request = new UpdateRequest(index, id).doc(builder);
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
    public void deleteByQuery(String index, Object data) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        Map<String, Object> dataMap = BeanUtils.parseClass(data);
        List<Object> list = dataMap.values().stream().filter(Objects::nonNull).collect(Collectors.toList());
        if(CollectionUtils.isEmpty(list)) {
            return ;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        dataMap.forEach((k,v)->{
            if(v!=null) {
                boolQueryBuilder.must(QueryBuilders.matchQuery(k,v));
            }
        });

        request.setQuery(boolQueryBuilder);
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
    public Boolean deleteByIndex(String index) {
        try {
            if(!this.exists(index)) {
                //不存在就结束
                return Boolean.TRUE;
            }
            //索引存在，就执行删除
            long s = System.currentTimeMillis();
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            request.timeout(new TimeValue(120, TimeUnit.SECONDS));
            AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
            long t = System.currentTimeMillis();
            //计算删除耗时
            log.info("删除索引:{},耗时{}",index,t-s);
        } catch (IOException e) {
            log.error(e.getMessage(),e);
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * 索引是否存在
     * @param indexName
     * @return
     */
    @Override
    public boolean exists(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            request.local(false);
            request.humanReadable(true);
            request.includeDefaults(false);
            return client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            return false;
        }
    }

    @Override
    public List searchDatePage(String index, int startPage, int pageSize,Object data) {

        List list = new ArrayList<>();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //设置根据哪个字段进行排序查询
//		sourceBuilder.sort(new FieldSortBuilder("birthday").order(SortOrder.DESC));
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        //添加查询条件
        Map<String, Object> dataMap = BeanUtils.parseClass(data);
        String[] fields = new String[dataMap.size()];
        AtomicInteger i = new AtomicInteger();
        dataMap.forEach((k,v)->{
            fields[i.getAndIncrement()] = k;
            if(v != null) {
                queryBuilder.must(QueryBuilders.matchQuery(k, v));
            }
        });

        //需要返回和不返回的字段，可以是数组也可以是字符串
        sourceBuilder.fetchSource(fields, null);

        SearchRequest request = new SearchRequest(index);
        //设置超时时间
        sourceBuilder.timeout(new TimeValue(120, TimeUnit.SECONDS));
        //设置是否按匹配度排序
        sourceBuilder.explain(true);
        //加载查询条件
        sourceBuilder.query(queryBuilder);
        // 设置一次取多少值 最大值1w 默认10
        sourceBuilder.size(10000);
        //设置分页 如果startPage <0  pageSize <0 不进行分页
        if(startPage > 0 && pageSize > 0) {
            sourceBuilder.from((startPage - 1) * pageSize).size(pageSize);
        }
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

    @Override
    @Async
    public String insertBatchAsyn(String index, List<EsEntity> list) {
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
    public List getAllByIndex(String index, Object data) {
        List list = new ArrayList<>();
        try {
            //构造查询条件
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder builder = new SearchSourceBuilder();

            //设置查询条件
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            Map<String, Object> dataMap = BeanUtils.parseClass(data);
            dataMap.forEach((k,v)->{
                if(v != null) {
                    queryBuilder.must(QueryBuilders.matchQuery(k, v));
                }
            });
            builder.query(queryBuilder);

            //设置查询超时时间
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2L));
            //设置最多一次能够取出10000笔数据，从第10001笔数据开始，将开启滚动查询  PS:滚动查询也属于这一次查询，只不过因为一次查不完，分多次查
            builder.size(10000);
            searchRequest.source(builder);
            //将滚动放入
            searchRequest.scroll(scroll);
            SearchResponse searchResponse = null;
            try {
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("查询索引库失败", e.getMessage(), e);
            }
            SearchHits hits = searchResponse.getHits();
            //记录要滚动的ID
            String scrollId = searchResponse.getScrollId();

            //TODO 对结果集的处理
            addList(list, hits);

            //滚动查询部分，将从第10001笔数据开始取
            SearchHit[] hitsScroll = hits.getHits();
            while (hitsScroll != null && hitsScroll.length > 0) {
                //构造滚动查询条件
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(scroll);
                try {
                    //响应必须是上面的响应对象，需要对上一层进行覆盖
                    searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    log.error("滚动查询失败", e.getMessage(), e);
                }
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits();
                hitsScroll = hits.getHits();

                //TODO 同上面完全一致的结果集处理
                addList(list, hits);
            }

            //清除滚动，否则影响下次查询
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = null;
            try {
                clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("滚动查询清除失败", e.getMessage(), e);
            }
            //清除滚动是否成功
            clearScrollResponse.isSucceeded();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }
        return list;
    }

    private void addList(List list, SearchHits hits) throws IOException {
        for(SearchHit searchHit : hits.getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            list.add(sourceAsMap);
        }
    }

}
