package com.example.myes.service;


import java.util.Map;

/**
 * @author lh
 * @version 1.0
 * @since 2022-0818
 */
public interface MappingService {





    /**
     * 获取Elasticsearch的数据转换后类型
     *
     * @param mysqlType mysql数据类型
     * @param data      具体数据
     * @return Elasticsearch对应的数据类型
     */
    Object getElasticsearchTypeObject(String mysqlType, String data);
}
