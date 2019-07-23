package com.blk.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.blk.canal.MyKafkaSender;
import com.blk.common.GmallConstants;

import java.util.List;

public class CanalHanlder {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDataList;

    public CanalHanlder(String tableName, CanalEntry.EventType EventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = EventType;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        //如果表名是order_info 并且操作是新增，insert 下单操作
        if("order_info".equals(tableName)&&eventType==CanalEntry.EventType.INSERT){

            //遍历行集
            for (CanalEntry.RowData rowData : rowDataList) {

                //获取修改后的行集
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName()+"<===>"+column.getValue());
                    jsonObject.put(column.getName(), column.getValue());
                }

                //往kafka生产数据，以json串的形式
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_NEW_ORDER, jsonObject.toJSONString());

            }

        }
    }
}
