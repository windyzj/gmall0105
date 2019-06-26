package com.atguigu.gmall0105.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0105.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        Long dauTotal = publisherService.getDauTotal(date);

        List  totalList =new ArrayList();

        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value", dauTotal );
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","newMid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value", 233 );
        totalList.add(newMidMap);


        return   JSON.toJSONString(totalList) ;
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id")String id ,@RequestParam("date") String tdate){
        if("dau".equals(id)){
            Map hourMap=new HashMap();
            Map dauHourTMap = publisherService.getDauHour(tdate);
            String ydate = getYdate(tdate);
            Map dauHourYMap = publisherService.getDauHour(ydate);
            hourMap.put("yesterday",dauHourYMap);
            hourMap.put("today",dauHourTMap);
            return JSON.toJSONString(hourMap);
        }
        return  null;
    }

    public String getYdate(String tDateStr){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String ydateStr=null;
        try {
              Date tdate = simpleDateFormat.parse(tDateStr);
              Date ydate = DateUtils.addDays(tdate, -1);
              ydateStr=simpleDateFormat.format(ydate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ydateStr;
    }

}
