package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.HourBean;
import com.atguigu.gmall.publisher.dao.Dau;
import com.atguigu.gmall.publisher.dao.Order;
import com.atguigu.gmall.publisher.service.Publish;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublishImpl implements Publish {

    @Autowired
    Dau dau;
    @Autowired
    Order order;

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
}
