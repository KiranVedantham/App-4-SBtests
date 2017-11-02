package com.swisscom.api.controllers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.swisscom.api.AllServiceConfiguration;
import com.swisscom.api.ResourceUtil;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping("/redis/slave1")
public class RedisSlaveOneController {

	@Autowired
	private AllServiceConfiguration sc;
	private static final String FILENAME = "Sample.csv";
	private static final String slave = "slave1";
	@RequestMapping(path = "/insertdata", method = RequestMethod.POST, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
//	public ResponseEntity<String> insertData() throws Exception {
//		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
//		jedis.set("Sample1", "Hello All This is Redis Sample");
//		return new ResponseEntity<String>(HttpStatus.OK);
//	}
	public ResponseEntity<Map<String,String>> insertData(@RequestBody Map<String,String> payload) throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		for (Map.Entry<String, String> entry : payload.entrySet())
		{
			jedis.set(entry.getKey(), entry.getValue());
		}
		return new ResponseEntity<Map<String,String>>(payload, HttpStatus.OK);
	}
	
	@RequestMapping(path = "/insertlist", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> insertList() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		//store data in redis list 
	      jedis.lpush("AppCloud-list", "Redis"); 
	      jedis.lpush("AppCloud-list", "Mongodb"); 
	      jedis.lpush("AppCloud-list", "Mysql"); 
		return new ResponseEntity<String>(HttpStatus.OK);
	}
	@RequestMapping(path = "/searchList", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> searchList() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		 List<String> list = jedis.lrange("AppCloud-list", 0 ,5); 
	      
	      for(int i = 0; i<list.size(); i++) { 
	         System.out.println("Stored string in AppCloud-list:: "+list.get(i)); 
	      } 
		return new ResponseEntity<String>(HttpStatus.OK);
	}
//	@RequestMapping(path = "/searchdata", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
    @SuppressWarnings("null")
//	public ResponseEntity<String> searchData() throws Exception {
//		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
//		System.out.println("Message returned : " + jedis.get("Sample1"));
//		return new ResponseEntity<String>(HttpStatus.OK);
//	}
	@RequestMapping(value = "/searchdata/{redisKey}", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<Map<String,String>> searchData(@PathVariable("redisKey") String redisKey) throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		Map<String,String> payload = new HashMap<String,String>() ;
		payload.put(redisKey, jedis.get(redisKey)) ;
		return new ResponseEntity<Map<String,String>>(payload,HttpStatus.OK);
	}

	@RequestMapping(path = "/load", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> redisLoad() throws Exception {
		String message = getdatafromfile(FILENAME);
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		for (int i = 0; i < 20; i++) {
			jedis.set(UUID.randomUUID().toString().replace("-", ""), message);
			System.out.println("MEssage load iteration"+i);
		}
		System.out.println("jedis.dbSize()"+jedis.dbSize());
		System.out.println("jedis.getDB().SIZE :"+jedis.getDB().SIZE);
		return new ResponseEntity<String>(HttpStatus.OK);
	}
	@RequestMapping(path = "/chksize", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> redisDbSize() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		System.out.println("jedis.getDB().SIZE :"+jedis.getDB().SIZE);
		return new ResponseEntity<String>(jedis.dbSize().toString(),HttpStatus.OK);
	}
	
	@RequestMapping(value  = "/info/{storge}", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> redisinfo(@PathVariable String storage) throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		System.out.println("jedis.info() :"+jedis.info());
		return new ResponseEntity<String>(jedis.info(),HttpStatus.OK);
	}
	@RequestMapping(path = "/info", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> redisinfo() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		System.out.println("jedis.info() :"+jedis.info());
		return new ResponseEntity<String>(jedis.info(),HttpStatus.OK);
	}
	
	
	@RequestMapping(path = "/loadsearch", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> loadSearch() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance(slave);
		for (int i = 0; i < 10; i++) {
			System.out.println("Message returned : " + jedis.get("Test"+i));
		}
		return new ResponseEntity<String>(HttpStatus.OK);
	}

	private String getdatafromfile(String filename2) throws Exception {
		ResourceUtil ru = new ResourceUtil();
		StringBuffer sb = new StringBuffer();
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(ru.getFile(filename2));
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(ru.getFile(FILENAME)));
			while ((sCurrentLine = br.readLine()) != null) {
				sb.append(sCurrentLine);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return sb.toString();
	}
	
	@RequestMapping(value = "/deletedata", method = RequestMethod.DELETE, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE }, consumes = {MediaType.ALL_VALUE})
	public ResponseEntity<Map<String, String>> deleteData(@RequestBody String keyForDelete) throws Exception {
		String deletedKeys = "0";
		Map<String, String> payload = new HashMap() ;
		try {
			Jedis jedis = (Jedis)sc.getServiceInstance(slave);
			deletedKeys = String.valueOf(jedis.del(keyForDelete));
			payload.put("Number of items deleted", deletedKeys) ;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		return new ResponseEntity<Map<String, String>> (payload, HttpStatus.OK);
	}
}
