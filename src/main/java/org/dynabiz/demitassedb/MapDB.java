package org.dynabiz.demitassedb;


import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class MapDB implements DB {
    private Map<String, ValueNode> storageMap = new ConcurrentHashMap<String, ValueNode>();
    private Timer expireTimer = new Timer();
    private static final long EXP_CHECK_INTERVAL = 5000; // 5s


    private static final ValueNode NULL_NODE = new ValueNode();

    public MapDB(){
        expireTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                expireCheck();
            }
        }, EXP_CHECK_INTERVAL);
    }

    public synchronized Object get(String key) {
        return mapGetNodeAfterTTL(key).value;
    }

    public synchronized void set(String key, Object value) {
        set(key, value, -1);
    }

    public synchronized void set(String key, Object value, long ttl) {
        if(ttl == 0) return;
        ValueNode node = new ValueNode();
        node.value = value;
        if(ttl > 0) node.expireTimestamp = System.currentTimeMillis() + ttl;
        else node.expireTimestamp = -1;
        storageMap.put(key, node);
    }

    public synchronized boolean setIfAbsent(String key, String value) {
        return setIfAbsent(key, value, -1);
    }

    public synchronized boolean setIfAbsent(String key, String value, long ttl) {
        if(exists(key)){
            return false;
        }
        set(key, value, ttl);
        return true;
    }

    public synchronized boolean updateExpire(String key, long ttl) {
        ValueNode node = mapGetNodeAfterTTL(key);
        if(node == NULL_NODE){
            return false;
        }
        node.expireTimestamp = System.currentTimeMillis() + ttl;
        return true;
    }

    public synchronized boolean exists(String key) {
        return mapGetNodeAfterTTL(key) != NULL_NODE;
    }

    public synchronized Object remove(String key) {
        ValueNode node = storageMap.remove(key);
        if(node != null) return node.value;
        return null;
    }

    private ValueNode mapGetNodeAfterTTL(String key){
        ValueNode node = storageMap.get(key);
        if(node == null) return NULL_NODE;
        if(node.expireTimestamp > 0 && System.currentTimeMillis() > node.expireTimestamp){
            storageMap.remove(key);
            return NULL_NODE;
        }
        return node;
    }

    private void expireCheck(){
        Iterator<Map.Entry<String, ValueNode>> iterator = storageMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, ValueNode> entry = iterator.next();
            if(entry.getValue().expireTimestamp < System.currentTimeMillis()){
                iterator.remove();
            }
        }
    }



}
