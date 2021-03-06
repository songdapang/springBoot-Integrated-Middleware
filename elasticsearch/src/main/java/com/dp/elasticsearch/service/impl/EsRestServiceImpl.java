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
//            log.info("??????:{},????????????,?????????:{},id:{}", index, response.status().getStatus(), Id);
        } catch (Exception e) {
            log.error("??????????????????,index:{},id:{}", index, id);
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
            log.info("????????????,?????????:{},id:{}", response.status().getStatus(), Id);
        } catch (Exception e) {
            log.error("??????????????????,index:{},id:{}", index, id);
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
            log.info("??????:{},????????????{}???????????????!", index, list.size());
        } catch (Exception e) {
            log.error("??????:{},????????????????????????", index);
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
        //?????????????????????????????????
        request.setBatchSize(1000);
        long deleted = 1;
        while (deleted > 0) {
            try {
                deleted = client.deleteByQuery(request, RequestOptions.DEFAULT).getDeleted();
                log.info("????????????????????????,index:{},num:{}", index,deleted);
            } catch (Exception e) {
                deleted = 1;
                log.error("??????????????????????????????,index:{}", index);
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
            log.info("??????:{},??????id{}????????????:{}", index, id, JSON.toJSONString(response));
        } catch (Exception e) {
            log.error("??????id??????????????????,index:{},id:{}", index, id);
        }
        return state;
    }

    @Override
    public Boolean deleteByIndex(String index) {
        try {
            if(!this.exists(index)) {
                //??????????????????
                return Boolean.TRUE;
            }
            //??????????????????????????????
            long s = System.currentTimeMillis();
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            request.timeout(new TimeValue(120, TimeUnit.SECONDS));
            AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
            long t = System.currentTimeMillis();
            //??????????????????
            log.info("????????????:{},??????{}",index,t-s);
        } catch (IOException e) {
            log.error(e.getMessage(),e);
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * ??????????????????
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
        //??????????????????????????????????????????
//		sourceBuilder.sort(new FieldSortBuilder("birthday").order(SortOrder.DESC));
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        //??????????????????
        Map<String, Object> dataMap = BeanUtils.parseClass(data);
        String[] fields = new String[dataMap.size()];
        AtomicInteger i = new AtomicInteger();
        dataMap.forEach((k,v)->{
            fields[i.getAndIncrement()] = k;
            if(v != null) {
                queryBuilder.must(QueryBuilders.matchQuery(k, v));
            }
        });

        //????????????????????????????????????????????????????????????????????????
        sourceBuilder.fetchSource(fields, null);

        SearchRequest request = new SearchRequest(index);
        //??????????????????
        sourceBuilder.timeout(new TimeValue(120, TimeUnit.SECONDS));
        //??????????????????????????????
        sourceBuilder.explain(true);
        //??????????????????
        sourceBuilder.query(queryBuilder);
        // ???????????????????????? ?????????1w ??????10
        sourceBuilder.size(10000);
        //???????????? ??????startPage <0  pageSize <0 ???????????????
        if(startPage > 0 && pageSize > 0) {
            sourceBuilder.from((startPage - 1) * pageSize).size(pageSize);
        }
        log.info("?????????????????????" + sourceBuilder.toString());
        request.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            long totalHits = searchResponse.getHits().getTotalHits().value;
            log.info("?????????{}?????????", totalHits);
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
            log.error("??????????????????{}?????????", index);
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
            log.info("??????:{},????????????{}???????????????!", index, list.size());
        } catch (Exception e) {
            log.error("??????:{},????????????????????????", index);
            log.error(e.getMessage(),e);
        }
        return state;
    }

    @Override
    public List getAllByIndex(String index, Object data) {
        List list = new ArrayList<>();
        try {
            //??????????????????
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder builder = new SearchSourceBuilder();

            //??????????????????
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            Map<String, Object> dataMap = BeanUtils.parseClass(data);
            dataMap.forEach((k,v)->{
                if(v != null) {
                    queryBuilder.must(QueryBuilders.matchQuery(k, v));
                }
            });
            builder.query(queryBuilder);

            //????????????????????????
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(2L));
            //??????????????????????????????10000??????????????????10001???????????????????????????????????????  PS:????????????????????????????????????????????????????????????????????????????????????
            builder.size(10000);
            searchRequest.source(builder);
            //???????????????
            searchRequest.scroll(scroll);
            SearchResponse searchResponse = null;
            try {
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("?????????????????????", e.getMessage(), e);
            }
            SearchHits hits = searchResponse.getHits();
            //??????????????????ID
            String scrollId = searchResponse.getScrollId();

            //TODO ?????????????????????
            addList(list, hits);

            //??????????????????????????????10001??????????????????
            SearchHit[] hitsScroll = hits.getHits();
            while (hitsScroll != null && hitsScroll.length > 0) {
                //????????????????????????
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(scroll);
                try {
                    //?????????????????????????????????????????????????????????????????????
                    searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    log.error("??????????????????", e.getMessage(), e);
                }
                scrollId = searchResponse.getScrollId();
                hits = searchResponse.getHits();
                hitsScroll = hits.getHits();

                //TODO ???????????????????????????????????????
                addList(list, hits);
            }

            //???????????????????????????????????????
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = null;
            try {
                clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                log.error("????????????????????????", e.getMessage(), e);
            }
            //????????????????????????
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
