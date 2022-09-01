package com.example.myes.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CanalTest {

    public static void main(String[] args) {
        String ip = "106.12.122.204";
        String destination = "example";
        //创建连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(ip, 11111), destination, "canal", "canal"
        );

        //进行连接
        canalConnector.connect();
        //进行订阅
        canalConnector.subscribe();

        int batchSize = 5 * 1024;
        //使用死循环不断的获取canal信息
        while (true) {
            //获取Message对象
            Message message = canalConnector.getWithoutAck(batchSize);
            long id = message.getId();
            int size = message.getEntries().size();

            System.out.println("当前监控到的binLog消息数量是：" + size);

            //判断是否有数据
            if (id == -1 || size == 0) {
                //如果没有数据，等待1秒
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //如果有数据，进行数据解析
                List<Entry> entries = message.getEntries();

                //遍历获取到的Entry集合
                for (Entry entry : entries) {
                    System.out.println("----------------------------------------");
                    System.out.println("当前的二进制日志的条目（entry）类型是：" + entry.getEntryType());

                    //如果属于原始数据ROWDATA，进行打印内容
                    if (entry.getEntryType() == EntryType.ROWDATA) {
                        try {
                            //获取存储的内容
                            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());

                            //打印事件的类型，增删改查哪种 eventType
                            System.out.println("事件类型是：" + rowChange.getEventType());

                            //打印改变的内容(增量数据)
                            for (RowData rowData : rowChange.getRowDatasList()) {
                                System.out.println("改变前的数据：" + rowData.getBeforeColumnsList());
                                System.out.println("改变后的数据：");
                                for (int i = 0; i < rowData.getAfterColumnsList().size(); i++) {
                                    CanalEntry.Column column= rowData.getAfterColumnsList().get(i);
                                    System.out.println("是否为主键:"+column.getIsKey()+"字段名:"+column.getName()+"---"+column.getValue());
                                }

                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            //canalConnector.rollback();
                        }
                    }
                }
                //消息确认已经处理了
                canalConnector.ack(id);
            }
        }
    }
}
