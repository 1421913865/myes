package com.example.myes.service.impl;

import com.example.myes.es.JacksonUtil;
import com.example.myes.service.ElasticsearchService;
import com.example.myes.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author lh
 * @version 1.0
 * @since 2022-0818
 */
@Service
@Slf4j
public class ElasticsearchServiceImpl implements ElasticsearchService {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServiceImpl.class);

//    @Resource
//    private TransportClient transportClient;
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    @Value("${size:10}")
    private Integer threadNumSize;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolExecutor;

    @Override
    public void insertById(String index, String type, String id, Map<String, Object> dataMap) {
//        transportClient.prepareIndex(index, type, id).setSource(dataMap).get();
        Map<String, Map<String, Object>> idDataMap=new HashMap<>();
        idDataMap.put(id,dataMap);
        List<Integer> ids=new ArrayList<>();
        ids.add(Integer.parseInt(id));
        insertDatas(idDataMap, index, 1, ids);
    }

    @Override
    public void batchInsertById(String index, String type, Map<String, Map<String, Object>> idDataMap) {
//        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
//
//        idDataMap.forEach((id, dataMap) -> bulkRequestBuilder.add(transportClient.prepareIndex(index, type, id).setSource(dataMap)));
//        try {
//            BulkResponse bulkResponse = bulkRequestBuilder.execute().get();
//            if (bulkResponse.hasFailures()) {
//                logger.error("elasticsearch批量插入错误, index=" + index + ", type=" + type + ", data=" + JsonUtil.toJson(idDataMap) + ", cause:" + bulkResponse.buildFailureMessage());
//            }
//        } catch (Exception e) {
//            logger.error("elasticsearch批量插入错误, index=" + index + ", type=" + type + ", data=" + JsonUtil.toJson(idDataMap), e);
//        }
    }

    @Override
    public void multithreadingBatchInsertById(String index, String type, Map<String, Map<String, Object>> idDataMap) {

        //线程
        int threadNum = threadNumSize;

        //每页多少数据量
        int size=2;
        //页数
        int totalPage = (int) Math.ceil((idDataMap.size() * 1.0) / size);
        List<Integer> ids=getIds(idDataMap);

        //每个线程  对应的页数
        Map<Integer, CopyOnWriteArrayList<List<Integer>>> everyThreadDealPage = everyThreadDealPage2(ids, size, threadNum);

        CountDownLatch encount = new CountDownLatch(totalPage);
        AtomicInteger page = new AtomicInteger(1);
        for (Map.Entry<Integer, CopyOnWriteArrayList<List<Integer>>> entry : everyThreadDealPage.entrySet()) {
            log.info("开启多线程.......");
            Integer key = entry.getKey();
            CopyOnWriteArrayList<List<Integer>> values = entry.getValue();

            threadPoolExecutor.setThreadNamePrefix(String.format("key->%s", key));
            for (int i = 0; i < values.size(); i++) {
                int finalI = i;
                CompletableFuture.runAsync(() -> {
                    try {
                        insertDatas(idDataMap, index, size, values.get(finalI));
                    } catch (Throwable e) {

                    }
                    log.info("总页数：totalPage：{}，处理第{}页数据；", totalPage, page.get());
                    log.info("page.intValue():{}", page.get());
                    page.set(page.get() + 1);

                }, threadPoolExecutor).whenComplete((unused, throwable) -> {
                    encount.countDown();
                    log.info("encount.countDown():{}", encount.getCount());
                    log.error("成功了，失败原因：{}", throwable.getMessage());
                });

            }

        }
        log.info("page.intValue2():{}", page.get());
        try {
            encount.await();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }


//        idDataMap.forEach((id, dataMap) -> bulkRequestBuilder.add(transportClient.prepareIndex(index, type, id).setSource(dataMap)));
//        try {
//            BulkResponse bulkResponse = bulkRequestBuilder.execute().get();
//            if (bulkResponse.hasFailures()) {
//                logger.error("elasticsearch批量插入错误, index=" + index + ", type=" + type + ", data=" + JsonUtil.toJson(idDataMap) + ", cause:" + bulkResponse.buildFailureMessage());
//            }
//        } catch (Exception e) {
//            logger.error("elasticsearch批量插入错误, index=" + index + ", type=" + type + ", data=" + JsonUtil.toJson(idDataMap), e);
//        }
    }

    private void insertDatas(Map<String, Map<String, Object>> idDataMap, String indexName, int size, List<Integer> ids) {
        BulkRequest bulkRequest = new BulkRequest(indexName);

        for (Integer id : ids) {

            IndexRequest indexRequest = new IndexRequest();
            String idStr=id.toString();
            indexRequest.id(idStr);
            indexRequest.source(idDataMap.get(idStr));
            bulkRequest.add(indexRequest);

        }

        final BulkResponse bulkItemResponses = Try.of(() -> restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT)).get();
        final Set<String> isFailedIds = Arrays.stream(bulkItemResponses.getItems()).filter(BulkItemResponse::isFailed)
                .map(i -> i.getFailure().getId())
                .collect(Collectors.toSet());
        List<Integer> ids2 = Lists.newArrayList();
        for (final Integer id : ids) {
            if (!isFailedIds.contains(id)) {
                ids2.add(id);
            }
        }

        log.info("写入es的id数量:{}......", (ids.size()-isFailedIds.size()));
        log.info("写入es失败的id数量:{}......", isFailedIds.size());

    }

    @Override
    public void update(String index, String type, String id, Map<String, Object> dataMap) {
        this.insertById(index, type, id, dataMap);
    }

    @Override
    public void deleteById(String indexName, String type, String id) {
//        transportClient.prepareDelete(index, type, id).get();
        DeleteRequest deleteRequest=new DeleteRequest(indexName);
        deleteRequest.id(id);
        try {
            restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            log.info("删除es的id为{}......成功", id);
        } catch (Exception e) {
            log.info("删除es的id为{}......失败", id);
            e.printStackTrace();
        }
    }
    public static Map<Integer, CopyOnWriteArrayList<List<Integer>>> everyThreadDealPage2(List<Integer> ids, int size, int threadNum) {
        int temp = 1;
        List<List<Integer>> partition = ListUtils.partition(ids, size);
        int ceil = (int) Math.ceil(partition.size() * 1.0 / threadNum);
        Map<Integer, CopyOnWriteArrayList<List<Integer>>> maps = Maps.newHashMapWithExpectedSize(threadNum);
        List<List<List<Integer>>> partitionThread = ListUtils.partition(partition, ceil<=0?100:ceil);
        for (List<List<Integer>> lists : partitionThread) {
            if (maps.containsKey(temp)) {
                maps.get(temp).addAll(lists);
            } else {
                maps.put(temp, new CopyOnWriteArrayList<>(lists));
            }
            if (temp == threadNum) {
                temp = 1;
            }
            temp = temp + 1;


        }

        return maps;
    }
    public static List<Integer> getIds(Map<String, Map<String, Object>> idDataMap){
        Iterator<Map.Entry<String, Map<String, Object>>> it = idDataMap.entrySet().iterator();
        List<Integer> ids=new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Object>> entry = it.next();
            ids.add(Integer.parseInt(entry.getKey()));
        }
        return ids;
    }
}
