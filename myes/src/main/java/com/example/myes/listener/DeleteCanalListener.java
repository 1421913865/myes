package com.example.myes.listener;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.example.myes.event.DeleteAbstractCanalEvent;
import com.example.myes.service.ElasticsearchService;
import com.example.myes.service.MappingService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Optional;

/**
 * @author lh
 * @version 1.0
 * @since 2022-0818
 */
@Component
public class DeleteCanalListener extends AbstractCanalListener<DeleteAbstractCanalEvent> {
    private static final Logger logger = LoggerFactory.getLogger(DeleteCanalListener.class);

    @Resource
    private MappingService mappingService;

    @Resource
    private ElasticsearchService elasticsearchService;

    @Override
    protected void doSync(String database, String table, String index, String type, RowData rowData) {
        List<Column> columns = rowData.getBeforeColumnsList();
        String primaryKey = "id";
        Column idColumn = columns.stream().filter(column -> column.getIsKey() && primaryKey.equals(column.getName())).findFirst().orElse(null);
        if (idColumn == null || StringUtils.isBlank(idColumn.getValue())) {
            logger.warn("insert_column_find_null_warn insert从column中找不到主键,database=" + database + ",table=" + table);
            return;
        }
        logger.debug("insert_column_id_info insert主键id,database=" + database + ",table=" + table + ",id=" + idColumn.getValue());
        elasticsearchService.deleteById(index, type, idColumn.getValue());
        logger.debug("insert_es_info 同步es插入操作成功！database=" + database + ",table=" + table + ",id=" + idColumn.getValue());
    }
}
