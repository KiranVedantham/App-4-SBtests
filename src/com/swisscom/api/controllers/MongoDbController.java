package com.swisscom.api.controllers;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.swisscom.api.AllServiceConfiguration;
import com.swisscom.api.ResourceUtil;

@RestController
@RequestMapping("/mongodb")
public class MongoDbController {
	private static final String FILENAME = "Sample.csv";
	@Autowired
	private AllServiceConfiguration sc;
	ResourceUtil rutil = new ResourceUtil();

	@RequestMapping(value = "/insertOne", method = RequestMethod.GET)
	public HttpStatus insertOne() throws Exception {
		MongoDatabase dbConnection = null;
		try {
			/**** Get database ****/
			dbConnection = (MongoDatabase) sc.getServiceInstance();
			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			MongoCollection<Document> table = dbConnection.getCollection("user");
			/**** Insert ****/
			// create a document to store key and value
			Document document = new Document();
			document.put("name", "AppCloud");
			document.put("lastname", "Swisscom");
			document.put("createdDate", new Date());
			table.insertOne(document);
			;
			System.out.println("Document  is Inserted!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/insertMany", method = RequestMethod.GET)
	public HttpStatus insertMany() throws Exception {
		MongoDatabase dbConnection = null;
		try {
			dbConnection = (MongoDatabase) sc.getServiceInstance();
			MongoCollection<Document> table = dbConnection.getCollection("user");

			List<Document> documents = new ArrayList<>();
			for (int i = 0; i < 500; i++) {
				Document document = new Document();
				document.put("name", "AppCloud" + i);
				document.put("lastname", "Swisscom" + i);
				document.put("createdDate", new Date());
				documents.add(document);
			}

			table.insertMany(documents);
			System.out.println("Documents 500 is Inserted!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/delete", method = RequestMethod.GET)
	public HttpStatus deleteRecords() throws Exception {
		MongoDatabase dbConnection = null;
		try {
			/**** Get database ****/
			dbConnection = (MongoDatabase) sc.getServiceInstance();
			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			MongoCollection<Document> table = dbConnection.getCollection("user");
			/**** Insert ****/
			// create a document to store key and value
			Bson filter = new Document("name", "AppCloud");
			table.deleteOne(filter);
			System.out.println("Document  is Deleted!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/update", method = RequestMethod.GET)
	public HttpStatus update() throws Exception {
		MongoDatabase dbConnection = null;
		try {
			/**** Get database ****/
			dbConnection = (MongoDatabase) sc.getServiceInstance();
			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			MongoCollection<Document> table = dbConnection.getCollection("user");
			/**** Insert ****/
			// create a document to store key and value
			Bson filter = new Document("name", "AppCloud2");
			Bson update = new Document("name", "Swisscom Changed");
			table.updateOne(filter, new Document("$set", update));
			System.out.println("Document  is updated!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/bulkwrite", method = RequestMethod.GET)
	public HttpStatus bulkwrite() throws Exception {
		MongoDatabase dbConnection = null;
		try {
			/**** Get database ****/
			dbConnection = (MongoDatabase) sc.getServiceInstance();
			/**** Get collection / table from 'testdb' ****/
			// if collection doesn't exists, MongoDB will create it for you
			MongoCollection<Document> table = dbConnection.getCollection("user");
			/**** Insert ****/
			// create a document to store key and value
			Bson filter = new Document("name", "AppCloud1");
			Bson update = new Document("name", "Swisscom Changed bulkoperation");
			Random rand = new Random();
			table.bulkWrite(Arrays.asList(new InsertOneModel<>(new Document("_id", rand.nextInt())),
					new InsertOneModel<>(new Document("_id", rand.nextInt())), new InsertOneModel<>(new Document("_id", rand.nextInt())),
					new UpdateOneModel<>(filter, new Document("$set", update)),
					new DeleteOneModel<>(new Document("_id", 2)),
					new ReplaceOneModel<>(new Document("_id", 3), new Document("_id", 3).append("x", 4))));
			System.out.println("BulkWrite!");

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/rsstatus", method = RequestMethod.GET, produces = "application/json")
	public String getUserInfo() throws Exception {
		DB dbConnection = null;
		CommandResult cr = null;
		try {
			dbConnection = (DB) sc.getMonogDBUserInfo();
			cr = dbConnection.command("replSetGetStatus");
			System.out.println("dbConnection.getStats()" + cr.toJson());
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return cr.toJson();
	}

	@RequestMapping(value = "/fetchAll", method = RequestMethod.GET)
	public HttpStatus fetchRecords() throws Exception {
		MongoDatabase dbConnection = null;
		try {
			dbConnection = (MongoDatabase) sc.getServiceInstance();
			MongoCollection<Document> table = dbConnection.getCollection("user");
			FindIterable<Document> documents = table.find();
			for (Document document : documents) {
				System.out.println("Document!" + document.toString());
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/deletecollection", method = RequestMethod.GET)
	public HttpStatus deleteCollection() throws Exception {
		DB dbConnection = null;
		dbConnection = (DB) sc.getMongoDBInstance1("mongodb");
		DBCollection table = dbConnection.getCollection("user");
		table.drop();
		System.out.println("Collection  is deleted!");
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/loadStorageCapacity", method = RequestMethod.GET)
	public HttpStatus storageCapacity() throws Exception {
		try {
			DB db = (DB) sc.getMongoDBInstance1("mongodb");
			DBCollection collection = db.getCollection("ImageCollection");
			ResourceUtil rUtil = new ResourceUtil();
			File imageFile = rUtil.getFile(FILENAME);
			// create a "photo" namespace
			GridFS gfsPhoto = new GridFS(db, "photo");
			for (int i = 0; i < 500; i++) {
				// get image file from local drive
				GridFSInputFile gfsFile = gfsPhoto.createFile(imageFile);
				// set a new filename for identify purpose
				gfsFile.setFilename(imageFile.getName());
				// save the image file into mongoDB
				gfsFile.save();
				System.out.println("Inserted file" + i);
			}
			System.out.println("Done  Now DB size is " + db.getStats().getDouble("dataSize")+ "bytes");

		} catch (UnknownHostException e) {
			e.printStackTrace();
			return HttpStatus.EXPECTATION_FAILED;
		} catch (MongoException e) {
			e.printStackTrace();
			return HttpStatus.EXPECTATION_FAILED;
		} catch (IOException e) {
			e.printStackTrace();
			return HttpStatus.EXPECTATION_FAILED;
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/failStorageCapacity", method = RequestMethod.GET)
	public HttpStatus failstorageCapacity() throws Exception {
		try {
			DB db = (DB) sc.getMongoDBInstance1("mongodb");
			DBCollection collection = db.getCollection("ImageCollection");
			ResourceUtil rUtil = new ResourceUtil();
			File imageFile = rUtil.getFile(FILENAME);
			// create a "photo" namespace
			GridFS gfsPhoto = new GridFS(db, "photo");
			for (int i = 0; i < 5; i++) {
				// get image file from local drive
				GridFSInputFile gfsFile = gfsPhoto.createFile(imageFile);
				// set a new filename for identify purpose
				gfsFile.setFilename(imageFile.getName());
				// save the image file into mongoDB
				gfsFile.save();
				System.out.println("Inserted file" + i);
			}
			System.out.println("Done  Now DB size is " + db.getStats().getDouble("dataSize")+ "bytes");
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return HttpStatus.EXPECTATION_FAILED;
		} catch (MongoException e) {
			e.printStackTrace();
			return HttpStatus.EXPECTATION_FAILED;
		} catch (IOException e) {
			e.printStackTrace();
			return HttpStatus.EXPECTATION_FAILED;
		}
		return HttpStatus.OK;
	}
}
