package com.swisscom.api.controllers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.swisscom.api.AllServiceConfiguration;
import com.swisscom.api.ResourceUtil;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping("/redis")
public class RedisController {

	@Autowired
	private AllServiceConfiguration sc;
	private static final String FILENAME = "Sample.csv";

	@RequestMapping(path = "/insertdata", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> insertData() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance();
		jedis.set("Sample1", "Hello All This is Redis Sample");
		return new ResponseEntity<String>(HttpStatus.OK);
	}
	@RequestMapping(path = "/insertlist", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> insertList() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance();
		//store data in redis list 
	      jedis.lpush("AppCloud-list", "Redis"); 
	      jedis.lpush("AppCloud-list", "Mongodb"); 
	      jedis.lpush("AppCloud-list", "Mysql"); 
		return new ResponseEntity<String>(HttpStatus.OK);
	}
	@RequestMapping(path = "/searchList", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> searchList() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance();
		 List<String> list = jedis.lrange("AppCloud-list", 0 ,5); 
	      
	      for(int i = 0; i<list.size(); i++) { 
	         System.out.println("Stored string in AppCloud-list:: "+list.get(i)); 
	      } 
		return new ResponseEntity<String>(HttpStatus.OK);
	}
	@RequestMapping(path = "/searchdata", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> searchData() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance();
		System.out.println("Message returned : " + jedis.get("Sample1"));
		return new ResponseEntity<String>(HttpStatus.OK);
	}

	@RequestMapping(path = "/load", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> redisLoad() throws Exception {
		String message = getdatafromfile(FILENAME);
		Jedis jedis = (Jedis)sc.getServiceInstance();
		for (int i = 0; i < 500; i++) {
			jedis.set("Test"+i, message);
			System.out.println("MEssage load iteration"+i);
		}
		System.out.println("jedis.dbSize()"+jedis.dbSize());
		System.out.println("jedis.getDB().SIZE :"+jedis.getDB().SIZE);
		return new ResponseEntity<String>(HttpStatus.OK);
	}
	
	@RequestMapping(path = "/loadsearch", method = RequestMethod.GET, produces = { MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> loadSearch() throws Exception {
		Jedis jedis = (Jedis)sc.getServiceInstance();
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
}
