package com.swisscom.api.controllers;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.swisscom.api.AllServiceConfiguration;
import com.unboundid.util.json.JSONObject;
import com.unboundid.util.json.JSONValue;

@RestController
@RequestMapping("/elk")
public class ElkController {

	@Autowired
    private AllServiceConfiguration sc;

		@RequestMapping(path = "/logSearchSTG", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> logSearchSTG() throws Exception {
		RestClient restClient = (RestClient) sc.getServiceInstance();
		DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
		// get current date time with Date()
		Date date = new Date();
		HttpEntity entity = new NStringEntity(
				"{\n" + "  \"query\": {\n" +
				        " \"simple_query_string\" : {\n " +
				            "\"fields\" : [\"syslog5424_proc\"],\n"+
				            "\"query\" : \"[CELL]\"\n"
				+ "                }\n" + "            }" + "        }\n" + "    }\n" + "}",
				ContentType.APPLICATION_JSON);

		String todayindex = "/parsed-" + dateFormat.format(date) + "/logs/_search";
			Response indexResponse = restClient.performRequest("GET", todayindex, Collections.<String, String>emptyMap(),
				entity);
		OutputStream om = new ByteArrayOutputStream();
		indexResponse.getEntity().writeTo(om);
		JSONObject myObject = new JSONObject(om.toString());
		System.out.println(myObject.getFields().toString());
		Map<String, JSONValue> hits = myObject.getFields();
		System.out.println(hits.get("hits"));
		JSONObject css = (JSONObject) hits.get("hits");
		Map<String, JSONValue> totalhits = css.getFields();
		System.out.println(totalhits.get("total"));

		if (totalhits.get("total") != null)

		{

			int a = Integer.parseInt(totalhits.get("total").toString());
			if(a<=0)
				return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);

		}
		return new ResponseEntity<String>(HttpStatus.OK);
	}
		@RequestMapping(path = "/logSearchCELL", method = RequestMethod.GET, produces = {
				MediaType.APPLICATION_JSON_UTF8_VALUE })
		public ResponseEntity<String> logSearchCELL() throws Exception {
			RestClient restClient = (RestClient) sc.getServiceInstance();
			DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
			// get current date time with Date()
			Date date = new Date();
			HttpEntity entity = new NStringEntity(
					"{\n" + "  \"query\": {\n" +
					        " \"simple_query_string\" : {\n " +
					            "\"fields\" : [\"syslog5424_proc\"],\n"+
					            "\"query\" : \"[CELL]\"\n"
					+ "                }\n" + "            }" + "        }\n" + "    }\n" + "}",
					ContentType.APPLICATION_JSON);

			String todayindex = "/parsed-" + dateFormat.format(date) + "/logs/_search";
				Response indexResponse = restClient.performRequest("GET", todayindex, Collections.<String, String>emptyMap(),
					entity);
			OutputStream om = new ByteArrayOutputStream();
			indexResponse.getEntity().writeTo(om);
			JSONObject myObject = new JSONObject(om.toString());
			System.out.println(myObject.getFields().toString());
			Map<String, JSONValue> hits = myObject.getFields();
			System.out.println(hits.get("hits"));
			JSONObject css = (JSONObject) hits.get("hits");
			Map<String, JSONValue> totalhits = css.getFields();
			System.out.println(totalhits.get("total"));

			if (totalhits.get("total") != null)

			{

				int a = Integer.parseInt(totalhits.get("total").toString());
				if(a<=0)
					return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);

			}
			return new ResponseEntity<String>(HttpStatus.NO_CONTENT);
		}
		@RequestMapping(path = "/logSearch", method = RequestMethod.GET, produces = {
				MediaType.APPLICATION_JSON_UTF8_VALUE })
		public ResponseEntity<String> logSearch() throws Exception {
			RestClient restClient = (RestClient) sc.getServiceInstance();
			DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
			// get current date time with Date()
			Date date = new Date();
			HttpEntity entity = new NStringEntity(
					"{\n" + "    \"query\": {\n" + "        \"filtered\" : {\n" + "            \"query\" : {\n"
							+ "                \"match\" : {\n" + "                    \"message\" : \"/Exit status 0\"\n"
							+ "                }\n" + "            }" + "        }\n" + "    }\n" + "}",
					ContentType.APPLICATION_JSON);

			String todayindex = "/parsed-" + dateFormat.format(date) + "/logs/_search";
				Response indexResponse = restClient.performRequest("GET", todayindex, Collections.<String, String>emptyMap(),
					entity);
			OutputStream om = new ByteArrayOutputStream();
			indexResponse.getEntity().writeTo(om);
			JSONObject myObject = new JSONObject(om.toString());
			System.out.println(myObject.getFields().toString());
			Map<String, JSONValue> hits = myObject.getFields();
			System.out.println(hits.get("hits"));
			JSONObject css = (JSONObject) hits.get("hits");
			Map<String, JSONValue> totalhits = css.getFields();
			System.out.println(totalhits.get("total"));

			if (totalhits.get("total") != null)

			{

				int a = Integer.parseInt(totalhits.get("total").toString());
				if(a<=0)
					return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);

			}
			return new ResponseEntity<String>(HttpStatus.OK);
		}
		
		@RequestMapping(path = "/logSearchAPP", method = RequestMethod.GET, produces = {
				MediaType.APPLICATION_JSON_UTF8_VALUE })
		public ResponseEntity<String> logSearchAPP() throws Exception {
			RestClient restClient = (RestClient) sc.getServiceInstance();
			DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
			// get current date time with Date()
			Date date = new Date();
			HttpEntity entity = new NStringEntity(
					"{\n" + "  \"query\": {\n" +
					        " \"simple_query_string\" : {\n " +
					            "\"fields\" : [\"syslog5424_proc\"],\n"+
					            "\"query\" : \"[APP]\"\n"
					+ "                }\n" + "            }" + "        }\n" + "    }\n" + "}",
					ContentType.APPLICATION_JSON);

			String todayindex = "/parsed-" + dateFormat.format(date) + "/logs/_search";
				Response indexResponse = restClient.performRequest("GET", todayindex, Collections.<String, String>emptyMap(),
					entity);
			OutputStream om = new ByteArrayOutputStream();
			indexResponse.getEntity().writeTo(om);
			JSONObject myObject = new JSONObject(om.toString());
			System.out.println(myObject.getFields().toString());
			Map<String, JSONValue> hits = myObject.getFields();
			System.out.println(hits.get("hits"));
			JSONObject css = (JSONObject) hits.get("hits");
			Map<String, JSONValue> totalhits = css.getFields();
			System.out.println(totalhits.get("total"));

			if (totalhits.get("total") != null)

			{

				int a = Integer.parseInt(totalhits.get("total").toString());
				if(a<=0)
					return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);

			}
			return new ResponseEntity<String>(HttpStatus.OK);
		}
		
		@RequestMapping(path = "/logSearchRTR", method = RequestMethod.GET, produces = {
				MediaType.APPLICATION_JSON_UTF8_VALUE })
		public ResponseEntity<String> logSearchRTR() throws Exception {
			RestClient restClient = (RestClient) sc.getServiceInstance();
			DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
			// get current date time with Date()
			Date date = new Date();
			HttpEntity entity = new NStringEntity(
					"{\n" + "  \"query\": {\n" +
					        " \"simple_query_string\" : {\n " +
					            "\"fields\" : [\"syslog5424_proc\"],\n"+
					            "\"query\" : \"[RTR]\"\n"
					+ "                }\n" + "            }" + "        }\n" + "    }\n" + "}",
					ContentType.APPLICATION_JSON);

			String todayindex = "/parsed-" + dateFormat.format(date) + "/logs/_search";
				Response indexResponse = restClient.performRequest("GET", todayindex, Collections.<String, String>emptyMap(),
					entity);
			OutputStream om = new ByteArrayOutputStream();
			indexResponse.getEntity().writeTo(om);
			JSONObject myObject = new JSONObject(om.toString());
			System.out.println(myObject.getFields().toString());
			Map<String, JSONValue> hits = myObject.getFields();
			System.out.println(hits.get("hits"));
			JSONObject css = (JSONObject) hits.get("hits");
			Map<String, JSONValue> totalhits = css.getFields();
			System.out.println(totalhits.get("total"));

			if (totalhits.get("total") != null)

			{

				int a = Integer.parseInt(totalhits.get("total").toString());
				if(a<=0)
					return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);

			}
			return new ResponseEntity<String>(HttpStatus.OK);
		}
	@RequestMapping(path = "/faillogSearch", method = RequestMethod.GET, produces = {
			MediaType.APPLICATION_JSON_UTF8_VALUE })
	public ResponseEntity<String> nologSearch() throws Exception {
		RestClient restClient = (RestClient) sc.getServiceInstance();

		DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
		// get current date time with Date()
		Date date = new Date();
		HttpEntity entity = new NStringEntity(
				"{\n" + "    \"query\": {\n" + "        \"filtered\" : {\n" + "            \"query\" : {\n"
						+ "                \"match\" : {\n" + "                    \"message\" : \"IamNotFind\"\n"
						+ "                }\n" + "            }" + "        }\n" + "    }\n" + "}",
				ContentType.APPLICATION_JSON);

		String todayindex = "/parsed-" + dateFormat.format(date) + "/logs/_search";
			Response indexResponse = restClient.performRequest("GET", todayindex, Collections.<String, String>emptyMap(),
				entity);
		OutputStream om = new ByteArrayOutputStream();
		indexResponse.getEntity().writeTo(om);
		JSONObject myObject = new JSONObject(om.toString());
		System.out.println(myObject.getField("message"));
		System.out.println(myObject.getFields().toString());
		Map<String, JSONValue> hits = myObject.getFields();
		System.out.println(hits.get("hits"));
		JSONObject css = (JSONObject) hits.get("hits");
		Map<String, JSONValue> totalhits = css.getFields();
		System.out.println(totalhits.get("total"));

		if (totalhits.get("total") != null)

		{

			int a = Integer.parseInt(totalhits.get("total").toString());
			if(a<=0)
				return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);

		}
		return new ResponseEntity<String>(HttpStatus.OK);
	}

}
