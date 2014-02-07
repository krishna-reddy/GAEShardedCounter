package com.adaptavant.shardingCounter;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;
import com.google.appengine.api.memcache.AsyncMemcacheService;
import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import java.io.InputStreamReader;


import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * This initial implementation simply counts all instances of the
 * SimpleCounterShard kind in the datastore. The only way to increment the
 * number of shards is to add another shard by creating another entity in the
 * datastore.
 */
@Controller
public class ShardedCounter {

    /**
     * DatastoreService object for Datastore access.
     */
    private static final DatastoreService DS = DatastoreServiceFactory
            .getDatastoreService();

    /**
     * Default number of shards.
     */
    private static final int NUM_SHARDS = 20;
    
    /**
     * Cache duration for memcache.
     */
    private static final int CACHE_PERIOD = 60;
    
    /**
     * Memcache service object for Memcache access.
     */
     // Using the asynchronous cache
    private final  AsyncMemcacheService asyncCache = MemcacheServiceFactory.getAsyncMemcacheService();

    /**
     * A random number generator, for distributing writes across shards.
     */
    private final Random generator = new Random();

    /**
     * A logger object.
     */
    private static final Logger LOG = Logger.getLogger(ShardedCounter.class
            .getName());

    /**
     * Increment the value of this sharded counter.
     */
	@RequestMapping(value="sharedcounter/createCounterEntry",method=RequestMethod.POST)
    public @ResponseBody String createCounterEntry(HttpServletRequest req) {
		//Declare and Initialize the JSON Object with null
		JSONObject json=null;
		
		//begin the transaction
		Transaction tx = DS.beginTransaction();
		
		//declare entity
		Entity shard;
		
		//declare key
		Key shardKey=null;
		
		//declare project id
		String projectIdentity=null;
		try {
		// 1.Get received JSON data from request
		JSONTokener jt=new JSONTokener(new InputStreamReader(req.getInputStream()));
		
		//2.Pass the JSONTokener to JSONObject
		json=new JSONObject(jt);
		
		//3.Get the values from json object using keys		
		projectIdentity=(String) json.get("projectIdentity");

        Query q = new Query(projectIdentity);
        // Use PreparedQuery interface to retrieve results
        PreparedQuery pq = DS.prepare(q);
   
        if(pq.countEntities(FetchOptions.Builder.withDefaults().limit(1))==0){
            int shardNum = generator.nextInt(NUM_SHARDS);
            
            shardKey = KeyFactory.createKey(projectIdentity,
                    Integer.toString(shardNum));
    		shard = new Entity(shardKey);
            shard.setUnindexedProperty("count", 0L);

            DS.put(tx, shard);
            tx.commit();
            
    	    json=new JSONObject();
    	    json.put("counterName", projectIdentity);
    	    json.put("count",0L);
        }
        else{
        json=new JSONObject();
	    json.put("counterName","Invalid");
        }

        } catch (Exception e) {
        	//log the error message
            LOG.log(Level.WARNING, e.toString(), e);
        	
        } finally {
        	//rollback the transaction if active
            if (tx.isActive()) {
                tx.rollback();
            }
        }
		return json.toString();
    }
	
    
    /**
     * Increment the value of this sharded counter.
     */
	@RequestMapping(value="sharedcounter/updateCounterEntry",method=RequestMethod.POST)
    public @ResponseBody String updateCounterEntry(HttpServletRequest req) {
		//Declare and Initialize the JSON Object with null
		JSONObject json=null;
		
		//begin the transaction
		Transaction tx = DS.beginTransaction();
		
		//declare entity
		Entity shard;
		
		Key shardKey=null;
		try {
		// 1.Get received JSON data from request
		JSONTokener jt=new JSONTokener(new InputStreamReader(req.getInputStream()));
		
		//2.Pass the JSONTokener to JSONObject
		json=new JSONObject(jt);
		
		//3.Get the values from json object using keys		
		String projectIdentity=(String) json.get("projectIdentity");
		
        Query q = new Query(projectIdentity);
               
        // Use PreparedQuery interface to retrieve results
        PreparedQuery pq = DS.prepare(q);
    	

        if(pq.countEntities(FetchOptions.Builder.withDefaults().limit(1))!=0)
        {
            getCount(projectIdentity);
            try {
            	int shardNum = generator.nextInt(NUM_SHARDS);
                
            	shardKey = KeyFactory.createKey(projectIdentity,
                        Integer.toString(shardNum));
                
            	
                shard = DS.get(tx, shardKey);
                Long count1 = (Long) shard.getProperty("count");
                shard.setUnindexedProperty("count", count1+1L);
                asyncCache.increment(projectIdentity, 1L);
                 }catch (EntityNotFoundException e) {
                    shard = new Entity(shardKey);
                    shard.setUnindexedProperty("count", 1L);
                    asyncCache.increment(projectIdentity, 1);
                 }
            DS.put(tx, shard);
            tx.commit();
           	Future<Object> futureValue = asyncCache.get(projectIdentity); // read from cache
           	Long value = (Long) futureValue.get();
            json=new JSONObject();
            json.put("counterName", projectIdentity);
            json.put("count",value);
          }else{
            json=new JSONObject();
            json.put("counterName", "Invalid");
              }
       
        } catch (Exception e) {
        	//log the error message
            LOG.log(Level.WARNING, e.toString(), e);
        } finally {
        	//rollback the transaction if fail
            if (tx.isActive()) {
                tx.rollback();
            }
        }
		return json.toString();
    }
	
	/**
     * decrease the value of this sharded counter.
     */
	@RequestMapping(value="sharedcounter/deleteCounterEntry",method=RequestMethod.POST)
    public @ResponseBody String deleteCounterEntry(HttpServletRequest req) {
		//Declare and Initialize the JSON Object with null
		JSONObject json=null;
		
		TransactionOptions options = TransactionOptions.Builder.withXG(true);
		
		Transaction tx = DS.beginTransaction(options);
		
		//initialize the sum
		Long sum = 0L;
		try {
		// 1.Get received JSON data from request
		JSONTokener jt=new JSONTokener(new InputStreamReader(req.getInputStream()));
		
		//2.Pass the JSONTokener to JSONObject
		json=new JSONObject(jt);
		
		//3.Get the values from json object using keys		
		String projectIdentity=(String) json.get("projectIdentity");
		
		//declare the entity;
        Entity shard;
        
        Key shardKey=null;
        
        Query q = new Query(projectIdentity);
              
        // Use PreparedQuery interface to retrieve results
        PreparedQuery pq = DS.prepare(q);
                
         if(pq.countEntities(FetchOptions.Builder.withDefaults().limit(1))!=0){
            sum=getCount(projectIdentity);
            if(sum!=0L)
               {
                try{
                	int shardNum = generator.nextInt(NUM_SHARDS);
                    
                    shardKey = KeyFactory.createKey(projectIdentity,
                            Integer.toString(shardNum));
                    shard = DS.get(tx, shardKey);
                    long count = (Long) shard.getProperty("count");
                    shard.setUnindexedProperty("count", count - 1L);
               		
               		asyncCache.increment(projectIdentity, -1L);
                    }catch(Exception e1){
                    	shard = new Entity(shardKey);
                        shard.setUnindexedProperty("count", -1L);
                        asyncCache.increment(projectIdentity, -1L);
                         }
                DS.put(tx, shard);
                tx.commit();
                Future<Object> futureValue = asyncCache.get(projectIdentity); // read from cache
                Long value = (Long) futureValue.get();                		
                json=new JSONObject();
                json.put("counterName", projectIdentity);
                json.put("count",value);
                }else{
                	json=new JSONObject();
            		json.put("counterName", projectIdentity);
            		json.put("count", 0L);
                	}
             }else{
                json=new JSONObject();
                json.put("counterName", "Invalid");
           
                }
       
        } catch (Exception e) {
        	//log the error message
            LOG.log(Level.WARNING, e.toString(), e);
        } finally {
        	//rollback the transaction if fails
            if (tx.isActive()) {
                tx.rollback();
            }
        }
		return json.toString();
    }
	
	
	/**
     * Retrieve the value of this sharded counter.
     *
     * @return Summed total of all shards' counts
     */
	@RequestMapping(value="/sharedcounter/getCount",method=RequestMethod.POST)
    public @ResponseBody String getCount(HttpServletRequest req) {
		//Declare and Initialize the JSON Object with null
		JSONObject json=null;
		
		//initialize the sum
		Long sum = 0L;
		try{
		// 1.Get received JSON data from request
		JSONTokener jt=new JSONTokener(new InputStreamReader(req.getInputStream()));
				
		//2.Pass the JSONTokener to JSONObject
		json=new JSONObject(jt);
				
		//3.Get the values from json object using keys		
		String projectIdentity=(String) json.get("projectIdentity");
		
		//query the datastore
		Query q = new Query(projectIdentity);
        
        // Use PreparedQuery interface to retrieve results
        PreparedQuery pq = DS.prepare(q);
        
        if(pq.countEntities(FetchOptions.Builder.withDefaults().limit(1))!=0)
        {
        	sum=getCount(projectIdentity);
        	json=new JSONObject();
        	json.put("counterName", projectIdentity);
        	json.put("count",sum);
        }else{
        	json=new JSONObject();
            json.put("counterName", "Invalid");
            
        }
		}catch(Exception e){
			//log the error message
			LOG.log(Level.WARNING, e.toString(), e);
		}
        return json.toString();
    }

	/**
     * Retrieve the value of this sharded counter.
     *
     * @return Summed total of all shards' counts
     */
    public final long getCount(String kind) {
       	Future<Object> futureValue = asyncCache.get(kind); // read from cache
       	Long value;
		try {
			value = (Long) futureValue.get();
			if (value != null) {
	            return value;
	        }
		} catch (InterruptedException | ExecutionException e) {
			//log the error message
			LOG.log(Level.WARNING, e.toString(), e);
		}
        

        long sum = 0;
        Query query = new Query(kind);
        for (Entity shard : DS.prepare(query).asIterable()) {
            sum += (Long) shard.getProperty("count");
        }
        asyncCache.put(kind, sum, Expiration.byDeltaSeconds(CACHE_PERIOD),
                SetPolicy.ADD_ONLY_IF_NOT_PRESENT);

        return sum;
    }
}