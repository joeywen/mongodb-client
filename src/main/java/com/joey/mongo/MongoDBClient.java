package com.joey.mongo;

import java.util.Map;

/**
 * @author joey 2013-09-02
 * @e-mail joey.wen@outlook.com
 */
public abstract class MongoDBClient {
  
  public abstract void insert(Map<String, String> map);

  public abstract String getValueByKey(String uid, String key);
  
  public abstract String getValueByKey(Map<String, String> map, String key);

  public abstract String get(String uid);

  public abstract void update(String uid, String key, String value);

  public abstract void update(String uid, String key, String src, String dest);
  
  public abstract void update(String uid, Map<String, String> map);

  public abstract void setDB(String dbName);
  
  public abstract void setCollection(String collName);
  
  public abstract void close();

  public abstract void connect();

  public class MongoDBException extends Exception {
    public MongoDBException(Exception e) {
      super(e);
    }
  }
}
