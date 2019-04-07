package io.dynabiz.demitassedb;

public interface DB {
    Object get(String key);
    void set(String key, Object value);
    void set(String key, Object value, long ttl);
    boolean setIfAbsent(String key, String value);
    boolean setIfAbsent(String key, String value, long ttl);
    boolean updateExpire(String key, long ttl);
    boolean exists(String key);
    Object remove(String key);
}
