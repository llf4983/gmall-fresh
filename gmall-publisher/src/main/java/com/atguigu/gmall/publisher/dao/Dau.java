package com.atguigu.gmall.publisher.dao;

import java.util.List;
import java.util.Map;

public interface Dau {
    public Long getTotal(String date);
    public List<Map> getHour(String date);
}
