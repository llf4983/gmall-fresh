package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.dao.Dau;
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
}
