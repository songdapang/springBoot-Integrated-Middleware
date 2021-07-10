package com.dp.elasticsearch.controller;

import com.dp.elasticsearch.entity.EsDemoEntity;
import com.dp.elasticsearch.service.IEsRestService;
import com.dp.elasticsearch.utils.BeanUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @Author songqinglong
 * @Date 2021/7/10 6:30 下午
 * @Description:对外暴露接口
 * @Version 1.0
 */
@RestController("/elasticsearch")
public class ViewController {

    @Autowired
    private IEsRestService esRestService;

    private static String index = "your_es_index";

    private static String id = UUID.randomUUID().toString();

    @GetMapping
    public List getAll(@RequestBody EsDemoEntity queryVo){
        return esRestService.getAllByIndex(index,queryVo);
    }

    @PostMapping
    public String insert(@RequestBody EsDemoEntity queryVo){
        return esRestService.add(queryVo,index, id);
    }

    @PutMapping
    public String modify(@RequestBody EsDemoEntity queryVo) {
        return esRestService.update(queryVo,index,id);
    }

    @DeleteMapping
    public void remove(@RequestBody EsDemoEntity queryVo) {
        esRestService.deleteByQuery(index,queryVo);
    }


}
