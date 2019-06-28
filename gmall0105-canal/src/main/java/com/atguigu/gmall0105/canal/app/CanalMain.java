package com.atguigu.gmall0105.canal.app;

public class CanalMain {

    public static void main(String[] args) {
        CanalClient.watch("hadoop1",11111,"example","gmall0105.order_info");
    }
}
