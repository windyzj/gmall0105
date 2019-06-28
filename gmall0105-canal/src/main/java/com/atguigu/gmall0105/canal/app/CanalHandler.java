package com.atguigu.gmall0105.canal.app;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public class CanalHandler {

    private List<CanalEntry.RowData> rowDatasList;
    String tableName;
    CanalEntry.EventType eventType;

    public  CanalHandler(List<CanalEntry.RowData> rowDatasList, String tableName, CanalEntry.EventType eventType) {
        this.rowDatasList = rowDatasList;
        this.tableName = tableName;
        this.eventType = eventType;
    }

    public  void handle(){
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName()+"--->"+column.getValue());
            }


        }



    }


}
