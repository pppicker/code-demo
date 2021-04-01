package com.example.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xyh
 * @date 2021/4/1 17:58
 */


//rides报错：
    //  redis.client.jedis.exception.JedisException: Could not get a resource from the pool


public class RedisUtil {

    /**
     * 获取redis对象
     *
     * @param redisClusterConfig redis集群配置
     * @return
     */
    public static JedisCluster getRedis(RedisClusterConfig redisClusterConfig) {
        JedisConnectionFactory jedisConnectionFactory = redisClusterConfig.jedisConnectionFactory();
        JedisCluster jedis = (JedisCluster) jedisConnectionFactory.getConnection().getNativeConnection();
        return jedis;
    }


    /**
     * redis模糊查询key值
     * @param redisService
     * @param key
     * @return
     */
    public static List<String> getScan(Jedis redisService, String key) {

        List<String> list = new ArrayList<String>();
        ScanParams params = new ScanParams();
        params.match(key);
        params.count(10000);
        String cursor = "0";
        while (true) {
            ScanResult scanResult = redisService.scan(cursor,params);
            List<String> elements = scanResult.getResult();
            if (elements != null && elements.size() > 0) {
                list.addAll(elements);
            }
            cursor = scanResult.getStringCursor();
            if ("0".equals(cursor)) {
                break;
            }
        }
        return list;
    }

    /**
     * redis模糊查询key值入口方法
     * @param jedisCluster
     * @param matchKey
     * @return List<key>
     */
    public static List<String> getRedisKeys(JedisCluster jedisCluster,String matchKey) {
        List<String> list = new ArrayList<String>();
        try {
            Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
            for (Map.Entry<String, JedisPool> entry : clusterNodes.entrySet()) {
                Jedis jedis = entry.getValue().getResource();
                // 判断非从节点(因为若主从复制，从节点会跟随主节点的变化而变化)
                if (!jedis.info("replication").contains("role:slave")) {
                    List<String> keys = getScan(jedis,matchKey);
                    if (keys.size() > 0) {
                        Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
                        for (String key : keys) {
                            // cluster模式执行多key操作的时候，这些key必须在同一个slot上，不然会报:JedisDataException:
                            // CROSSSLOT Keys in request don't hash to the same slot
                            int slot = JedisClusterCRC16.getSlot(key);
                            // 按slot将key分组，相同slot的key一起提交
                            if (map.containsKey(slot)) {
                                map.get(slot).add(key);
                            } else {
                                map.put(slot, Arrays.asList(key));

                            }
                        }
                        for (Map.Entry<Integer, List<String>> integerListEntry : map.entrySet()) {
                            // System.out.println("integerListEntry="+integerListEntry);
                            list.addAll(integerListEntry.getValue());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        return list;
    }
}
