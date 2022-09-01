package com.example.myes.es;

import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class InsertUtil {
    @Autowired
    private ThreadPoolTaskExecutor threadPoolExecutor;

    @Autowired
    private RestHighLevelClient restHighLevelClient;
    public void insertes(int start,int end){
        BulkRequest bulkRequest = new BulkRequest("legislation");

        List<String> dataList=new ArrayList<>();
        double total=0;
        for (int i = start; i < end; i++) {
            String data="{\"trackers\":\"Became Law\",\"committeeNames\":[],\"legislationRelationList\":[{\"relatedLegislation\":\"87-H.R.8895\",\"legislationId\":5273603,\"id\":252300}],\"reportNumbers\":[],\"subjects\":[],\"printNumbers\":[],\"title\":\"Joint resolution to amend the joint resolution providing for membersbip and participation by the United States in the Inter\",\"content\":[{\"key\":\"Text_PDF\",\"content\":\"{\\\"default\\\": \\\"https://www.congress.gov/87/statute/STATUTE-75/STATUTE-75-Pg784.pdf\\\"}\"},{}],\"congressNumber\":87,\"latestAction\":\"10/04/1961 Became Public Law No. 87-365\",\"congressDescription\":\"87th Congress (1961-1962)\",\"bilType\":\"Law\",\"id\":"+i+",\"billNumber\":\"S.J.Res.66\",\"actions\":[{\"actionTime\":\"1961-10-04 00:00:00\",\"billId\":5273603,\"description\":\"Became Public Law No. 87-365\",\"id\":1811288},{\"actionTime\":\"1961-09-18 00:00:00\",\"billId\":5273603,\"description\":\"Passed/agreed to in House.\",\"id\":1811289},{\"actionTime\":\"1961-03-24 00:00:00\",\"billId\":5273603,\"description\":\"Passed Senate/agreed to in Senate.\",\"id\":1811290},{\"actionTime\":\"1961-03-15 00:00:00\",\"billId\":5273603,\"description\":\"Introduced in Senate\",\"id\":1811291}]}";
            int length = data.getBytes(StandardCharsets.UTF_8).length;
           double size= getPrintSize2(length);
            total+=size;
            dataList.add(data);
        }
        System.out.println(total);
        for (String json : dataList) {

            IndexRequest indexRequest = new IndexRequest();
            final JsonNode jsonNode = JacksonUtil.readJson(json);
            if (jsonNode.hasNonNull("id")) {
                indexRequest.id(jsonNode.get("id").toString());
                final JsonNode id = jsonNode.get("id");
            }
            indexRequest.source(json, XContentType.JSON);

            bulkRequest.add(indexRequest);
        }

        final BulkResponse bulkItemResponses = Try.of(() -> restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT)).get();
        System.out.println(123);
    }
    public void duoxiancheng(){
        CountDownLatch encount = new CountDownLatch(100);
        AtomicInteger page = new AtomicInteger(1);
        for (int i = 0; i < 100; i++) {
            int start=i*200;
            int end=(i+1)*200;
            CompletableFuture.runAsync(() -> {
                insertes(start,end);

                log.info("总页数：totalPage：{}，处理第{}页数据；", 200, page.get());
                log.info("page.intValue():{}", page.get());
                page.getAndIncrement();


            }, threadPoolExecutor).whenComplete((unused, throwable) -> {
                encount.countDown();
                log.info("encount.countDown():{}", encount.getCount());
                log.error("成功了，失败原因：{}", throwable.getMessage());
            });

        }
    }
    public static double getPrintSize2(long size) {

        double kb = Math.ceil((size * 1.0) / 1024);
        return kb;
    }
    public void deleteIndex(String indexName){
        try {
            boolean exists = restHighLevelClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (exists) {
                log.info("索引存在，开始删除......");
                restHighLevelClient.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void createIndex(String indexName){
        refreshIndex(indexName);
        log.info("导入修改index setting：{}", indexName);

        Settings settings = Settings.builder().put("refresh_interval", -1).build();
        try {
            restHighLevelClient.indices().putSettings(
                    new UpdateSettingsRequest(indexName).settings(settings).indicesOptions(IndicesOptions.lenientExpandOpen())
                            .setPreserveExisting(false).timeout(TimeValue.timeValueMinutes(2)),

                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void  refreshIndex(String indexName){
        try {
            restHighLevelClient.indices().refresh(new RefreshRequest(indexName).indicesOptions(IndicesOptions.lenientExpandOpen())


                    , RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void updateIndex(String indexName){
        log.info("结束修改index setting：{}", indexName);
        //设置六十秒后自动刷新数据
        Settings settings = Settings.builder().put("refresh_interval", "60s").build();
        try {
            restHighLevelClient.indices().putSettings(
                    new UpdateSettingsRequest(indexName).settings(settings).indicesOptions(IndicesOptions.lenientExpandOpen())
                            .setPreserveExisting(false).timeout(TimeValue.timeValueMinutes(2)),

                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
