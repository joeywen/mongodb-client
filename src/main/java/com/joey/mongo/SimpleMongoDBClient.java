package com.joey.mongo;

import java.net.UnknownHostException;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * SimpleMongoDBClient
 * @author Joey
 * @e-mail joey.wen@outlook.com
 */
public class SimpleMongoDBClient extends MongoDBClient {

  String host = null;
  int port = 0;
  String dbName = null;
  Mongo mongo = null;
  DB db = null;
  DBCursor cursor = null;
  DBCollection collection = null;
  String collectionName = null;
  final static String DEFAULT_UIDNAME = "default";
  
  public SimpleMongoDBClient(String host, int port, String dbName, String collectionName) {
    this.host =host;
    this.port = port;
    this.dbName = dbName;
    this.collectionName = collectionName;
  }

  @Override
  public void connect() {
    try{  
      mongo = new Mongo(host, port);              
      if (mongo == null) 
        throw new NullPointerException("access mongoDB " + host + ":" + port + " error");
      db = mongo.getDB(dbName);
      if (db == null) 
        throw new NullPointerException("get db " + dbName + " error");
      collection = db.getCollection(collectionName);
      if( collection == null ) 
        throw new NullPointerException("get collection " + dbName + " error");
    } catch (UnknownHostException e) {
        System.err.println(e.getMessage());
        return;
    } catch (MongoException e) {
        System.err.println(e.getMessage());
        return;
    }
  }
  
  @Override
  public void setDB(String collName) {
    dbName = collName;
    if (mongo != null) {
      db = mongo.getDB(dbName);
    } else {
      throw new RuntimeException("access mongoDB " + host + ":" + port + " error");
    }
  }
  
  @Override
  public void setCollection(String collName) {
    // TODO Auto-generated method stub
    collectionName = collName;
    collection = db.getCollection(collectionName);
    if (collection == null) {
      throw new NullPointerException("get collection " + collectionName + " error");
    }
  }

  @Override
  public void close() {
    if (mongo != null) mongo.close();
  }
  
  /**
   * get the whole json string by uid
   * @param uid
   */
  @Override
  public String get(String uid) {
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put(DEFAULT_UIDNAME, uid);
    cursor = collection.find(searchQuery);
    
    if (cursor.count() > 1) {
      throw new RuntimeException("More than one result from find method with uid " + uid);
    }
    
    String ret = null;
    
    while (cursor.hasNext()) {
      ret = cursor.next().toString(); // json string
      break;
    }
    
    cursor.close();
    return ret;
  }
  
  /**
   * get the value of the key
   * @param uid
   */
  @Override
  public String getValueByKey(String uid, String key) {
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put(DEFAULT_UIDNAME, uid);
    cursor = collection.find(searchQuery);
    
    if (cursor.count() > 1) {
      throw new RuntimeException("More than one result from find method with uid " + uid);
    }
    
    return get(cursor, key);
  }
  
  @Override
  public String getValueByKey(Map<String, String> queryMap, String key) {
    BasicDBObject searchQuery = new BasicDBObject(queryMap);
    cursor = collection.find(searchQuery);
    
    if (cursor.count() > 1) {
      throw new RuntimeException("More than one result from find" +
          " method with query fields " + queryMap.toString());
    }
    
    return get(cursor, key);
  }

  private String get(DBCursor cur, String key) {
    String ret = null;
    while (cur !=null && cur.hasNext()) {
      DBObject obj = cur.next();
      if (obj != null) 
        ret = (String) obj.get(key); 
      break;
    }
    cur.close();
    return ret;
  }
  
  /**
   * @param uid
   */
  @Override
  public void update(String uid, String key, String value) {
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put(DEFAULT_UIDNAME, uid);
    cursor = collection.find(searchQuery);
    
    while (cursor.hasNext()) {
      cursor.next();
      BasicDBObject set = new BasicDBObject("$set", new BasicDBObject(key, value));
      collection.update(cursor.curr(), set);
      break;
    }
    cursor.close();
  }
  
  /**
   * find the value of the key, if the value equals src, set it to dest
   * 
   * @param uid
   *  
   */
  @Override
  public void update(String uid, String key, String src, String dest) {
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put(DEFAULT_UIDNAME, uid);
    cursor = collection.find(searchQuery);
    
    while (cursor.hasNext()) {
      if (cursor.next().get(key).equals(src)){
        BasicDBObject set = new BasicDBObject("$set", new BasicDBObject(key, dest));
        collection.update(cursor.curr(), set);
        break;
      }
    }
    cursor.close();
  }

  @Override
  public void update(String uid, Map<String, String> mm) {
    BasicDBObject searchQuery = new BasicDBObject();
    searchQuery.put(DEFAULT_UIDNAME, uid);
    cursor = collection.find(searchQuery);
    
    while (cursor.hasNext()) {
      cursor.next();
      BasicDBObject set = new BasicDBObject("$set", new BasicDBObject(mm));
      collection.update(cursor.curr(), set);
      break;
    }
    
    cursor.close();
  }
  
  @Override
  public void insert(Map<String, String> map) {
    collection.insert(new BasicDBObject(map));
  }

}
