package com.blk.canal.cilent;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.blk.canal.handler.CanalHanlder;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {

        //创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true){
            //连接
            canalConnector.connect();
            //抓取的表
            canalConnector.subscribe("gmall_0218.*");

            //抓取
            Message message = canalConnector.get(100);

            if(message.getEntries().size()==0){
                System.out.println("没有数据，休息一下！");

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //每一个entry对应一个SQL
                    //过滤一下entry ，因为不是每个SQL都是对数据的写操作，比如开关事务
                    if(CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){
                        CanalEntry.RowChange rowChange = null;

                        try {
                            //把storeValue进行反序列化，得到rowChange
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        //获取rowDatasList  行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //时间的类型 // insert  update delete drop alter
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取表名
                        String tableName = entry.getHeader().getTableName();

                        CanalHanlder canalHanlder = new CanalHanlder(tableName, eventType, rowDatasList);

                        canalHanlder.handle();


                    }
                }
            }

        }

    }
}
