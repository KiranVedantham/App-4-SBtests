package com.swisscom.api.controllers;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.swisscom.api.AllServiceConfiguration;
import com.swisscom.api.ResourceUtil;

@RestController
@RequestMapping("/mariaDb")
public class MariaDbController {

	@Autowired
	private AllServiceConfiguration sc;
	ResourceUtil rutil = new ResourceUtil();
	private static final String FILENAME = "Sample.csv";
	@RequestMapping(value = "/createtable", method = RequestMethod.GET)
	public String createTable(Model model) throws Exception {
		deleteTable();
		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		String createTableSQL = "CREATE TABLE Persons(" + "id int NOT NULL AUTO_INCREMENT,"
				+ "lastname varchar(255) NOT NULL," + "firstname varchar(255)," + "age varchar(255),"
				+ "email varchar(255)," + "Photo LONGBLOB," + " PRIMARY KEY (ID)" + ")";

		try {
			dbConnection = (Connection) sc.getServiceInstance();
			preparedStatement = dbConnection.prepareStatement(createTableSQL);
			System.out.println(createTableSQL);
			preparedStatement.executeUpdate();
			System.out.println("Table \"Persons\" is created!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		model.addAttribute("db", "Maria");
		return "/Home.html";
	}

	@RequestMapping(value = "/insert", method = RequestMethod.GET)
	public HttpStatus insertRecords() throws Exception {

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		String insertTableSQL = "INSERT INTO Persons" + "(lastname, firstname, age,email) VALUES" + "(?,?,?,?)";

		try {
			dbConnection = (Connection) sc.getServiceInstance();
			preparedStatement = dbConnection.prepareStatement(insertTableSQL);
			preparedStatement.setString(1, "Kiran");
			preparedStatement.setString(2, "Vedantham");
			preparedStatement.setString(3, "36");
			preparedStatement.setString(4, "kk@kmail.com");
			System.out.println(preparedStatement.toString());
			preparedStatement.executeUpdate();
			System.out.println("Table \"Persons\" is created!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/update", method = RequestMethod.GET)
	public HttpStatus updateRecords() throws Exception {

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		String updateTableSQL = "UPDATE Persons SET email = ? " + " WHERE firstname = 'Vedantham' ";

		try {
			dbConnection = (Connection) sc.getServiceInstance();
			preparedStatement = dbConnection.prepareStatement(updateTableSQL);
			preparedStatement.setString(1, "changedEmail@gmail.com");

			System.out.println(preparedStatement.toString());
			preparedStatement.executeUpdate();
			System.out.println("Table \"Persons\" is updated!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/delete", method = RequestMethod.GET)
	public HttpStatus deleteRecords() throws Exception {

		Statement statement = null;
		Connection dbConnection = null;
		String deleteTableSQL = "DELETE * from Persons";

		try {
			dbConnection = (Connection) sc.getServiceInstance();
			statement = dbConnection.prepareStatement(deleteTableSQL);
			System.out.println(statement.toString());
			statement.execute(deleteTableSQL);
			System.out.println("Table \"Persons\" records are deleted!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		return HttpStatus.OK;
	}
	@RequestMapping(value = "/deleteTable", method = RequestMethod.GET)
	public HttpStatus deleteTable() throws Exception {

		Statement statement = null;
		Connection dbConnection = null;
		String deleteTableSQL = "drop table if exists Persons";

		try {
			dbConnection = (Connection) sc.getServiceInstance();
			statement = dbConnection.prepareStatement(deleteTableSQL);
			System.out.println(statement.toString());
			statement.execute(deleteTableSQL);
			System.out.println("Table \"Persons\" is deleted!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/fetch", method = RequestMethod.GET)
	public HttpStatus fetchRecords() throws Exception {

		Statement statement = null;
		Connection dbConnection = null;
		String fetchTableSQL = "Select * from Persons";

		try {
			dbConnection = (Connection) sc.getServiceInstance();
			statement = dbConnection.prepareStatement(fetchTableSQL);
			System.out.println(statement.toString());
			ResultSet rs = statement.executeQuery(fetchTableSQL);
			while (rs.next()) {

				String firstname = rs.getString("firstname");
				String lastname = rs.getString("lastname");
				String email = rs.getString("email");

				System.out.println("firstname : " + firstname);
				System.out.println("lastname : " + lastname);
				System.out.println("lastname : " + email);

			}

			System.out.println("Table \"Persons\" records are Fetched!");
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}

		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/load", method = RequestMethod.GET)
	public HttpStatus loadRecords() throws Exception {

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		dbConnection = (Connection) sc.getServiceInstance();

		String insertTableSQL = "INSERT INTO Persons" + "(lastname, firstname, age,email) VALUES" + "(?,?,?,?)";
		for (int i = 1; i < 100; i++) {
			try {

				preparedStatement = dbConnection.prepareStatement(insertTableSQL);
				preparedStatement.setString(1, "Kiran " + i);
				preparedStatement.setString(2, "Vedantham " + i);
				preparedStatement.setString(3, "36");
				preparedStatement.setString(4, i + "kk@kmail.com");
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}

		}
		System.out.println("Table \"Persons\" loaded 100 records!");
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
		return HttpStatus.OK;
	}

	@RequestMapping(value = "/storageCapacity", method = RequestMethod.GET)
	public HttpStatus storageCapacity() throws Exception {

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		dbConnection = (Connection) sc.getServiceInstance();
		   FileInputStream fis = null;
			String insertTableSQL = "INSERT INTO Persons" + "(lastname, firstname, age,email,Photo) VALUES" + "(?,?,?,?,?)";
		 File file = rutil.getFile(FILENAME);
		for (int i = 0; i < 1000; i++) {
			try {
			      fis = new FileInputStream(file);
				preparedStatement = dbConnection.prepareStatement(insertTableSQL);
				preparedStatement.setString(1, "Kiran " + i);
				preparedStatement.setString(2, "Vedantham " + i);
				preparedStatement.setString(3, "36");
				preparedStatement.setString(4, i + "kk@kmail.com");
				preparedStatement.setAsciiStream(5,fis, (int) file.length());
				preparedStatement.executeUpdate();
				System.out.println("Table \"Persons\" inseted file "+file.getName()+i);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}

		}
		System.out.println("Table \"Persons\" inseted file "+file.getName());
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
		return HttpStatus.OK;
	}
	@RequestMapping(value = "/storageCapacity/{size}", method = RequestMethod.GET)
	public HttpStatus feeddb(@PathVariable String size) throws Exception {

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		dbConnection = (Connection) sc.getServiceInstance();
		   FileInputStream fis = null;
		   int mb = Integer.parseInt(size);
			String insertTableSQL = "INSERT INTO Persons" + "(lastname, firstname, age,email,Photo) VALUES" + "(?,?,?,?,?)";
		 File file = rutil.getFile(FILENAME);
		 System.out.println(file.length());
		for (int i = 1; i < mb; i++) {
			try {
			      fis = new FileInputStream(file);
				preparedStatement = dbConnection.prepareStatement(insertTableSQL);
				preparedStatement.setString(1, "Kiran " + i);
				preparedStatement.setString(2, "Vedantham " + i);
				preparedStatement.setString(3, "36");
				preparedStatement.setString(4, i + "kk@kmail.com");
				preparedStatement.setAsciiStream(5,fis, (int) file.length());
				preparedStatement.executeUpdate();
				System.out.println("Table \"Persons\" inserted file "+file.getName()+i);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}

		}
		System.out.println("Table \"Persons\" inseted file "+file.getName());
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
		return HttpStatus.OK;
	}
	@RequestMapping(value = "/failstorageCapacity", method = RequestMethod.GET)
	public HttpStatus failstorageCapacity() throws Exception {

		PreparedStatement preparedStatement = null;
		Connection dbConnection = null;
		dbConnection = (Connection) sc.getServiceInstance();
		   FileInputStream inputstream = null;
			String insertTableSQL = "INSERT INTO Persons" + "(lastname, firstname, age,email,Photo) VALUES" + "(?,?,?,?,?)";
		 File file = rutil.getFile(FILENAME);
		for (int i = 1; i < 5; i++) {
			try {
				inputstream = new FileInputStream(file);
				preparedStatement = dbConnection.prepareStatement(insertTableSQL);
				preparedStatement.setString(1, "Kiran " + i);
				preparedStatement.setString(2, "Vedantham " + i);
				preparedStatement.setString(3, "36");
				preparedStatement.setString(4, i + "kk@kmail.com");
				preparedStatement.setAsciiStream(5,inputstream, (int) file.length());
				preparedStatement.executeUpdate();
				System.out.println("Table \"Persons\" inseted file "+file.getName()+i);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				return HttpStatus.EXPECTATION_FAILED;
			}

		}
		System.out.println("Table \"Persons\" inseted file "+file.getName());
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (dbConnection != null) {
			dbConnection.close();
		}
		return HttpStatus.OK;
	}
}
