package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.HourBean;
import com.atguigu.gmall.publisher.dao.Dau;
import com.atguigu.gmall.publisher.dao.Order;
import com.atguigu.gmall.publisher.service.Publish;
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
public class PublishImpl implements Publish {

    @Autowired
    Dau dau;
    @Autowired
    Order order;
    
    @Autowired
    JestClient jestClient;

    @Override
    public Long getTotal(String date) {
        return dau.getTotal(date);
    }

    @Override
    public Map<String, Long> getHour(String date) {
        HashMap<String, Long> result = new HashMap<>();
        List<Map> hourListMap = dau.getHour(date);
        for (Map map : hourListMap) {
            String loghour = (String)map.get("LOGHOUR");
            Long count=(Long)map.get("CT");
            result.put(loghour,count);
        }
        return result;
    }

    @Override
    public Double orderTotal(String date) {
        return order.getTol(date);

    }

    @Override
    public Map<String, Double> orderHour(String date) {
        Map<String, Double> stringDoubleMap = new HashMap<>();
        List<HourBean> hous = order.getHou(date);
        for (HourBean hourBean : hous) {
            stringDoubleMap.put(hourBean.getCreateHour(),hourBean.getAmount());
        }
        return stringDoubleMap;
    }

    @Override
    public Map<String,Object> getES(String date, String keywords, int pageNo, int pageSize) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder dt = new TermQueryBuilder("dt",date);
        boolQueryBuilder.filter(dt);
        MatchQueryBuilder sku_name = new MatchQueryBuilder("sku_name", keywords).operator(MatchQueryBuilder.Operator.AND);
        boolQueryBuilder.must(sku_name);

        searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder gender = AggregationBuilders.terms("group_by_gender").field("user_gender").size(2);
        TermsBuilder age = AggregationBuilders.terms("group_by_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(gender);
        searchSourceBuilder.aggregation(age);
        
        searchSourceBuilder.from((pageNo-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        HashMap<String, Object> finalResultMap = new HashMap<>();
        try {
            SearchResult result = jestClient.execute(search);
            Long total = result.getTotal();
            finalResultMap.put("total",total);

            List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
            ArrayList hitsList = new ArrayList<Map>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                hitsList.add(hit.source);
            }
            finalResultMap.put("hits",hitsList);

            Map genderResultMap = new HashMap<String,Long>();
            List<TermsAggregation.Entry> genderBuckets = result.getAggregations().getTermsAggregation("group_by_gender").getBuckets();
            for (TermsAggregation.Entry genderBucket : genderBuckets) {
                String key = genderBucket.getKey();
                Long count = genderBucket.getCount();
                genderResultMap.put(key,count);
            }
            finalResultMap.put("gender",genderResultMap);

            Map ageResultMap = new HashMap<String,Long>();
            List<TermsAggregation.Entry> ageBuckets = result.getAggregations().getTermsAggregation("group_by_age").getBuckets();
            for (TermsAggregation.Entry ageBucket : ageBuckets) {
                String key = ageBucket.getKey();
                Long count = ageBucket.getCount();
                ageResultMap.put(key,count);
            }
            finalResultMap.put("age",ageResultMap);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return finalResultMap;
    }


}
