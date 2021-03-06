package com.example.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xyh
 * @date 2020/12/4 14:44
 */

public class JmxMgr {
    private static Logger log = LoggerFactory.getLogger(JmxMgr.class);
    private static List<JmxConnection> conns = new ArrayList<>();

    public static boolean init(List<String> ipPortList, boolean newKafkaVersion){
        for(String ipPort:ipPortList){
            log.info("init jmxConnection [{}]",ipPort);
            JmxConnection conn = new JmxConnection(newKafkaVersion, ipPort);
            boolean bRet = conn.init();
            if(!bRet){
                log.error("init jmxConnection error");
                return false;
            }
            conns.add(conn);
        }
        return true;
    }

    public static long getMsgInCountPerSec(String topicName){
        long val = 0;
        for(JmxConnection conn:conns){
            long temp = conn.getMsgInCountPerSec(topicName);
            val += temp;
        }
        return val;
    }

    public static double getMsgInTpsPerSec(String topicName){
        double val = 0;
        for(JmxConnection conn:conns){
            double temp = conn.getMsgInTpsPerSec(topicName);
            val += temp;
        }
        return val;
    }

    public static Map<Integer, Long> getEndOffset(String topicName){
        Map<Integer,Long> map = new HashMap<>();
        for(JmxConnection conn:conns){
            Map<Integer,Long> tmp = conn.getTopicEndOffset(topicName);
            if(tmp == null){
                log.warn("get topic endoffset return null, topic {}", topicName);
                continue;
            }
            for(Integer parId:tmp.keySet()){//change if bigger
                if(!map.containsKey(parId) || (map.containsKey(parId) && (tmp.get(parId)>map.get(parId))) ){
                    map.put(parId, tmp.get(parId));
                }
            }
        }
        return map;
    }

    public static void main(String[] args) {
        List<String> ipPortList = new ArrayList<>();
        ipPortList.add("55.11.59.173:9999");
        JmxMgr.init(ipPortList,false);

        String topicName = "mmgr_test";
        System.out.println(getMsgInCountPerSec(topicName));
        System.out.println(getMsgInTpsPerSec(topicName));
        System.out.println(getEndOffset(topicName));
    }
}
