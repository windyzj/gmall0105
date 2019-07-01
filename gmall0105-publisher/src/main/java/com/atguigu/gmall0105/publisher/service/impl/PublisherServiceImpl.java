package com.atguigu.gmall0105.publisher.service.impl;

import com.atguigu.gmall.constant.GmallConstants;
import com.atguigu.gmall0105.publisher.mapper.DauMapper;
import com.atguigu.gmall0105.publisher.mapper.OrderMapper;
import com.atguigu.gmall0105.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

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


    public Map getSaleDetail(String date,String keyword,int startpage,int size ){
String query="{\n" +
        "  \"query\": {\n" +
        "    \"bool\": {\n" +
        "      \"filter\": {\n" +
        "        \"term\": {\n" +
        "          \"dt\": \"2019-07-01\"\n" +
        "        }\n" +
        "      },\n" +
        "      \"must\":{\n" +
        "        \"match\":{\n" +
        "          \"sku_name\": {\n" +
        "            \"query\": \"小米手机\",\n" +
        "            \"operator\": \"and\"\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"aggs\": {\n" +
        "    \"groupby_gender\": {\n" +
        "      \"terms\": {\n" +
        "        \"field\": \"user_gender\",\n" +
        "        \"size\": 2\n" +
        "      }\n" +
        "    },\n" +
        "    \"groupby_age\": {\n" +
        "      \"terms\": {\n" +
        "        \"field\": \"user_age\",\n" +
        "        \"size\": 100\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"from\": 5,   \n" +
        "  \"size\": 5\n" +
        "  \n" +
        "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询部分
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //过滤
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        //全文匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder groupbyGenders = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        TermsBuilder groupbyAges = AggregationBuilders.terms("groupby_age").field("user_age").size(100);

        searchSourceBuilder.aggregation(groupbyGenders);
        searchSourceBuilder.aggregation(groupbyAges);

        searchSourceBuilder.from((startpage-1) *size);
        searchSourceBuilder.size( size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_SALE_DETAIL).addType("_doc").build();

        Long total =0L; //总数
        List<Map> saleDetailList=new ArrayList<>(); //明细

        //聚合结果 1 年龄 2 性别
        Map ageMap=new HashMap();
        Map genderMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            total=searchResult.getTotal();  //总数
            //明细
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                saleDetailList.add(hit.source);
            }
            //性别聚合结果
            List<TermsAggregation.Entry> bucketsGender = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry bucket : bucketsGender) {
                genderMap.put( bucket.getKey(),bucket.getCount());
            }
            //年龄聚合结果
            List<TermsAggregation.Entry> bucketsAge = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry bucket : bucketsAge) {
                ageMap.put( bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map saleMap=new HashMap();
        saleMap.put("total",total);
        saleMap.put("detail",saleDetailList);
        saleMap.put("ageMap",ageMap);
        saleMap.put("genderMap",genderMap);

        return saleMap;
    }






}
