package com.swisscom.api;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.util.json.JSONException;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.client.MongoDatabase;
import com.mysql.jdbc.Driver;
import com.rabbitmq.client.ConnectionFactory;
import com.swisscom.api.model.ElasticCredentials;

import redis.clients.jedis.Jedis;

@SuppressWarnings("unused")
@Configuration
public class AllServiceConfiguration {
	@Value("${vcap.services:}")
	//private String vcapServices ="{'nova_redis': [      {        'credentials': {          'host': 'kubernetes-service-node.service.consul',          'port': 47998,          'master_port': 47998,          'slave_ports': [   47750,            35803          ],          'password': 'Zvp5srqmWoiTl3VYYEUzvIpd6Gt50m'        },        'syslog_drain_url': null,        'volume_mounts': [],        'label': 'nova_redis',        'provider': null,        'plan': 'small',        'name': 'smallredinova',        'tags': []      }    ]  }";
	private String vcapServices ;
//	@Value("${vcap.}")
	private String jdbcurl;
	private String database;
	private String label;
	private String database_uri;
	@Value("${cred1:}")
	private String host;
	@Value("${cred2:0}")
	private int port;
	private String accessHost;
	private String accessKey;
	private String sharedSecret;
	private String uri;
	private String vhost;
	@Value("${cred3:}")
	private String password;
	private ElasticCredentials elasticCred;
	private String ops_manager_url;
	private String ops_manager_user;
	private String ops_manager_password;
	@Value("${cred4:0}")
	private int slaveport1;
	@Value("${cred5:0}")
	private int slaveport2;
	private String opt ="default";
	@Value("${cred6:}")
	private String dbLabel;

	public Object getServiceInstance(String option) throws Exception {
		String serviceLable = getDBlabel();
		Object obj = getConntoServiceInstance(serviceLable,option);
		return obj;

	}
	public Object getServiceInstance() throws Exception {
		String serviceLable = getDBlabel();
		Object obj = getConntoServiceInstance(serviceLable,opt);
		return obj;

	}

	private Object getConntoServiceInstance(String serviceLable,String option) throws JSONException, SQLException, Exception {
		Object obj = null;
		switch (serviceLable) {
		case "mariadb":
			System.out.println("Creating MariaDB Connection!");
			obj = getMariaDBInstance(serviceLable);
			break;
		case "mariadbent":
			System.out.println("Creating MariaDBEnt Connection!");
			obj = getMariaDBEntInstance(serviceLable);
			break;
		case "mongodb":
			System.out.println("Creating MongoDB Connection!");
			obj = getMongoDBInstance(serviceLable);
			break;
		case "dynstrg":
			System.out.println("Creating Dynstrg Connection!");
			obj = getAmazonS3(serviceLable);
			break;
		case "dynstrg_ECS":
			System.out.println("Creating Dynstrg ECS Connection!");
			obj = getAmazonS3(serviceLable);
			break;
		case "rabbitmq":
			System.out.println("Creating  Rabbitmq Connection!");
			obj = getRabitMQInstance(serviceLable);
			break;
		case "elk":
			System.out.println("Creating Elk Connection!");
			obj = getElkCredntials(serviceLable);
			break;
		case "redis":
			System.out.println("Creating Redis Connection!");
			obj = getRedisInstance(serviceLable,option);
			break;
		case "mongodbent":
			System.out.println("Creating Mongodbent Connection!");
			obj = getMongoDBEntInstance(serviceLable);
			break;
		case "rabbitmqent":
			System.out.println("Creating Rabbitmqent Connection!");
			obj = getRabitMQInstance(serviceLable);
			break;
		case "redisent":
			System.out.println("Creating Redisent Connection!");
			obj = getRedisInstance(serviceLable,option);
			break;
		default:
			System.out.println("No Connection created");
			break;
		}

		return obj;
	}

	private Object getMariaDBEntInstance(String serviceLable) throws JSONException, SQLException {

		setCredentials(serviceLable);
		Connection connection = null;
		Driver myDriver = new com.mysql.jdbc.Driver();
		DriverManager.registerDriver(myDriver);
		connection = DriverManager.getConnection(jdbcurl);
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}

		return connection;

	}

	private Object getElkCredntials(String serviceLabel) throws JSONException {
		setCredentials(serviceLabel);

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(elasticCred.elasticSearchUsername, elasticCred.elasticSearchPassword));

		RestClient restClient = RestClient
				.builder(new HttpHost(elasticCred.getElasticSearchHost(), elasticCred.elasticSearchPort))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				}).build();
		return restClient;
	}

	private Object getAmazonS3(String serviceLabel) throws JSONException {
		setCredentials(serviceLabel);
		BasicAWSCredentials awscredtials = new BasicAWSCredentials(accessKey, sharedSecret);
		AmazonS3Client amazonS3Client = new AmazonS3Client(awscredtials);
		amazonS3Client.setEndpoint(accessHost);
		return amazonS3Client;
	}

	private Object getMongoDBEntInstance(String serviceLabel) throws JSONException, UnknownHostException {
		setCredentials(serviceLabel);
		MongoClientURI mongo = new MongoClientURI(database_uri);
		System.out.println("database_uri  : " + mongo.getHosts().toString());
		@SuppressWarnings("resource")
		MongoClient mclient = new MongoClient(mongo);
		MongoDatabase mongodb = mclient.getDatabase(database);
		DB db = mclient.getDB(database);
		System.out.println("MongoDB  Connection Created" + db.command("replSetGetStatus"));
		System.out.println("MongoDB  Connection Created");
		return mongodb;

	}

	private Object getRabitMQInstance(String serviceLable) throws JSONException, KeyManagementException,
			NoSuchAlgorithmException, URISyntaxException, IOException, TimeoutException {

		setCredentials(serviceLable);
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(uri);
		com.rabbitmq.client.Connection connection = factory.newConnection();
		System.out.println("RabbitMQ Conneciton Estabilished : Connection Address :" + connection.hashCode());
		return connection;
	}

	private Object getRedisInstance(String serviceLable,String slave) throws JSONException {
		Jedis jedis =null;
		if (host.isEmpty()) {
			setCredentials(serviceLable);
		}
		if (slave.equalsIgnoreCase("slave1")){
			System.out.println("Connecting to  :" +host+  "  slaveport1 : "+slaveport1);
			 jedis = new Jedis(host, slaveport1,600);
			jedis.auth(password);
		}else if  (slave.equalsIgnoreCase("slave2")){
			System.out.println("Connecting to  :" +host+  "  slaveport2 : "+slaveport2);
			 jedis = new Jedis(host, slaveport2,600);
				jedis.auth(password);
		}else {
			System.out.println("Connecting to  :" +host+  "  Port : "+port);
			jedis = new Jedis(host, port,600);
				jedis.auth(password);
		}
			
		System.out.println("Redis  Connection Created :" + jedis.toString());
		return jedis;
	}
	private Object getMongoDBInstance(String serviceLable) throws JSONException, IOException {
		setCredentials(serviceLable);
		MongoClientURI mongo = new MongoClientURI(database_uri);
		MongoClientOptions.Builder options_builder = new MongoClientOptions.Builder();
		options_builder.maxConnectionIdleTime(60000);
		MongoClientOptions options = options_builder.build();
		MongoClient mclient = new MongoClient(mongo);
		MongoDatabase mongodb = mclient.getDatabase(database);
		System.out.println("MongoDB  Connection Created");
		return mongodb;
	}

	@SuppressWarnings({ "resource", "deprecation" })
	public Object getMongoDBInstance1(String serviceLable) throws JSONException, IOException {
		setCredentials(serviceLable);
		MongoClientURI mongo = new MongoClientURI(database_uri);
		MongoClient mclient = new MongoClient(mongo);
		DB db = mclient.getDB(database);
		return db;
	}

	private void setCredentials(String servicename) throws JSONException {
		CloudfoundEnvironmentHelper helper = new CloudfoundEnvironmentHelper();
		Map<String, Object> credentials = CloudfoundEnvironmentHelper.parseCredentials(vcapServices, Optional.empty())
				.get();

		if (servicename.equalsIgnoreCase("mariadb")) {
			this.jdbcurl = (String) credentials.get("jdbcUrl");
			this.database = (String) credentials.get("database");
		}

		if (servicename.equalsIgnoreCase("mariadbent")) {
			this.jdbcurl = (String) credentials.get("jdbcUrl");
			this.database = (String) credentials.get("database");
		}
		if (servicename.equalsIgnoreCase("mongodb")) {
			this.database_uri = (String) credentials.get("database_uri");
			this.database = (String) credentials.get("database");
		}
		if (servicename.equalsIgnoreCase("mongodbent")) {
			this.database_uri = (String) credentials.get("database_uri");
			this.database_uri = getdetails(database_uri, "mongodb");
			this.database = (String) credentials.get("database");
			this.database_uri = this.database_uri + "/" + this.database;
			this.ops_manager_url = (String) credentials.get("ops_manager_url");
			this.ops_manager_user = (String) credentials.get("ops_manager_user");
			this.ops_manager_password = (String) credentials.get("ops_manager_password");
		}
		if (servicename.equalsIgnoreCase("elk")) {
			elasticCred = new ElasticCredentials();
			this.elasticCred.setElasticSearchUsername((String) credentials.get("elasticSearchUsername"));
			this.elasticCred.setElasticSearchPassword((String) credentials.get("elasticSearchPassword"));
			this.elasticCred.setElasticSearchHost((String) credentials.get("elasticSearchHost"));
			this.elasticCred.setElasticSearchPort((int) credentials.get("elasticSearchPort"));
		}
		if (servicename.equalsIgnoreCase("dynstrg")) {
			this.accessHost = (String) credentials.get("accessHost");
			this.accessKey = (String) credentials.get("accessKey");
			this.sharedSecret = (String) credentials.get("sharedSecret");

		}
		if (servicename.equalsIgnoreCase("dynstrg_ECS")) {
			this.accessHost = (String) credentials.get("accessHost");
			this.accessKey = (String) credentials.get("accessKey");
			this.sharedSecret = (String) credentials.get("sharedSecret");
		}
		if (servicename.equalsIgnoreCase("rabbitmq")) {
			this.uri = (String) credentials.get("uri");
			this.host = (String) credentials.get("host");
			this.vhost = (String) credentials.get("vhost");
		}
		if (servicename.equalsIgnoreCase("rabbitmqent")) {
			this.uri = (String) credentials.get("uri");
			this.host = (String) credentials.get("host");
			this.vhost = (String) credentials.get("vhost");
		}
		if (servicename.equalsIgnoreCase("redis")) {
			this.host = (String) credentials.get("host");
			this.port = (int) credentials.get("port");
			this.password = (String) credentials.get("password");
		}
		if (servicename.equalsIgnoreCase("nova_redis")) {
			if (this.host.isEmpty()) {
				this.host = (String) credentials.get("host");
				this.port = (int) credentials.get("port");
				this.password = (String) credentials.get("password");
				ArrayList  slaves=  (ArrayList) credentials.get("slave_ports");
				this.slaveport1 = (int) slaves.get(0);
				this.slaveport2 = (int) slaves.get(1);
				System.out.println("Slave ports  slaveport1 :" +slaveport1);
				System.out.println("Slave ports  slaveport2 :" +slaveport2);
			}
		}
		if (servicename.equalsIgnoreCase("redisent")) {
			this.host = (String) credentials.get("host");
			this.port = (int) credentials.get("port");
			this.password = (String) credentials.get("password");

		}
	}

	public String getDBlabel() throws JSONException {
		String dbLabel;
		if (this.dbLabel.isEmpty()) {
			dbLabel = CloudfoundEnvironmentHelper.parseLabels(vcapServices);
		} else {
			dbLabel = this.dbLabel ;
		}
		return dbLabel;
	}

	private Object getMariaDBInstance(String serviceLable) throws JSONException, SQLException {
		setCredentials(serviceLable);
		Connection connection = null;
		Driver myDriver = new com.mysql.jdbc.Driver();
		DriverManager.registerDriver(myDriver);
		connection = DriverManager.getConnection(jdbcurl);
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}

		return connection;
	}

	private static String getdetails(String database_uri, String to_check) {
		String[] out = database_uri.split(",");
		for (int i = 0; i < out.length; i++) {
			String stri = out[i];
			if (stri.contains(to_check))
				database_uri = stri;
		}
		if (database_uri.contains("-1.service.consul")) {
			database_uri = database_uri.replace("-1.service.consul", "-0.service.consul");
		}
		if (database_uri.contains("-2.service.consul")) {
			database_uri = database_uri.replace("-2.service.consul", "-0.service.consul");
		}
		return database_uri;
	}

	public Object getMonogDBUserInfo() throws Exception {
		Object obj = null;
		String serviceLable = getDBlabel();
		obj = getMongoDBInstance1(serviceLable);
		return obj;

	}
}
