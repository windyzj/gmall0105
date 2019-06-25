package com.atguigu.gmall0105.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import   com.atguigu.gmall.constant.GmallConstants;

@RestController //Controller+responsebody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    public String doLog(@RequestParam("log") String log ){

        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts",System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();
        logger.info(jsonObject.toJSONString());


        // 2 推送到kafka
        if( "startup".equals( jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";
    }



}
