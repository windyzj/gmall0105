package com.atguigu.gmall0105.publisher.service.impl;

import com.atguigu.gmall0105.publisher.mapper.DauMapper;
import com.atguigu.gmall0105.publisher.mapper.OrderMapper;
import com.atguigu.gmall0105.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> mapList = dauMapper.selectDauHourMap(date);
        Map dauHourMap=new HashMap();
        for (Map map : mapList) {
            dauHourMap.put(map.get("LOGHOUR"), map.get("CT"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
        Map orderAmountHourMap=new HashMap();
        for (Map map : mapList) {
            orderAmountHourMap.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;
    }


}
