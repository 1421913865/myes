package com.example.myes.controller;

import com.example.myes.es.InsertUtil;
import com.example.myes.service.ElasticsearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/es")
public class EsController {
    @Autowired
    InsertUtil insertUtil;
    @Autowired
    ElasticsearchService elasticsearchService;
    //加入实体变量
    @GetMapping("/insertes")
    public String insert(){
        insertUtil.createIndex("legislation");
        insertUtil.refreshIndex("legislation");
        insertUtil.duoxiancheng();
        insertUtil.updateIndex("legislation");
        insertUtil.refreshIndex("legislation");
        return "success";
    }

}
