package com.atguigu.gmall0105.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0105.publisher.bean.Option;
import com.atguigu.gmall0105.publisher.bean.OptionGroup;
import com.atguigu.gmall0105.publisher.bean.SaleInfo;
import com.atguigu.gmall0105.publisher.service.PublisherService;
import jdk.nashorn.internal.runtime.options.Options;
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

        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);

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
        }else if("order_amount".equals(id)){
            Map hourMap=new HashMap();
            Map orderHourTMap = publisherService.getOrderAmountHour(tdate);
            String ydate = getYdate(tdate);
            Map orderHourYMap = publisherService.getOrderAmountHour(ydate);
            hourMap.put("yesterday",orderHourYMap);
            hourMap.put("today",orderHourTMap);
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


    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date")String date ,@RequestParam("startpage") int startpage,@RequestParam("size") int size,@RequestParam("keyword")String keyword){
        Map saleMap = publisherService.getSaleDetail(date, keyword, startpage, size);
        Long total = (Long)saleMap.get("total");
        List<Map> saleDetailList = (List)saleMap.get("detail");
        Map ageMap =(Map) saleMap.get("ageMap");
        Map genderMap =(Map) saleMap.get("genderMap");



        //  genderMap 整理成为  OptionGroup
        Long femaleCount =(Long) genderMap.get("F");
        Long maleCount =(Long) genderMap.get("M");
        double femaleRate = Math.round(femaleCount * 1000D / total) / 10D;
        double maleRate = Math.round(maleCount * 1000D / total) / 10D;
        List<Option> genderOptions=new ArrayList<>();
        genderOptions.add( new Option("男", maleRate));
        genderOptions.add( new Option("女", femaleRate));
        OptionGroup genderOptionGroup = new OptionGroup("性别占比", genderOptions);
        //  ageMap 整理成为  OptionGroup

        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;


        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String agekey =(String) entry.getKey();
            int age = Integer.parseInt(agekey);
            Long ageCount =(Long) entry.getValue();
            if(age <20){
                age_20Count+=ageCount;
            }else   if(age>=20&&age<30){
                age20_30Count+=ageCount;
            }else{
                age30_Count+=ageCount;
            }
        }

        Double age_20rate=0D;
        Double age20_30rate=0D;
        Double age30_rate=0D;

          age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
          age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
          age30_rate = Math.round(age30_Count * 1000D / total) / 10D;
        List<Option> ageOptions=new ArrayList<>();
        ageOptions.add( new Option("20岁以下",age_20rate));
        ageOptions.add( new Option("20岁到30岁",age20_30rate));
        ageOptions.add( new Option("30岁以上",age30_rate));
        OptionGroup ageOptionGroup = new OptionGroup("年龄占比", ageOptions);

        List<OptionGroup> optionGroupList=new ArrayList<>();
        optionGroupList.add(genderOptionGroup);
        optionGroupList.add(ageOptionGroup);

        SaleInfo saleInfo = new SaleInfo(total, optionGroupList, saleDetailList);

        return  JSON.toJSONString(saleInfo);
    }

}
