package com.example.service;

import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

/**
 * @author xyh
 * @date 2020/8/14 17:09
 */
public class AssetIndicator {

    @Scheduled(cron = "table.querry.count.quartz")
    public void tableQuerryCounts() throws Exception {
        //从mds desktop_document2_ss 表中查询所有数据
        //只查true的
//        List ssList = mapper.selectAll();

        //调用sql解析接口，返回time,id,[s.t, s1.t1, s2.t2]

    }
}
