package com.atguigu.gmall0105.publisher.service;

import java.util.Map;

public interface PublisherService  {


    public  Long getDauTotal(String date);

    public Map  getDauHour(String date );

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);

    public Map getSaleDetail(String date,String keyword,int startpage,int size );


    }
