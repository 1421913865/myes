package com.example.myes.service;

import java.util.Map;

/**
 * @author lh
 * @version 1.0
 * @since 2022-0818
 */
public interface ElasticsearchService {
    void insertById(String index, String type, String id, Map<String, Object> dataMap);

    void batchInsertById(String index, String type, Map<String, Map<String, Object>> idDataMap);

    void multithreadingBatchInsertById(String index, String type, Map<String, Map<String, Object>> idDataMap);

    void update(String index, String type, String id, Map<String, Object> dataMap);

    void deleteById(String index, String type, String id);
}
