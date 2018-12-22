package com.shiva;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;



public class JdbcTest {

	public static void main(String[] args) {

		Properties prop = new Properties();
		try {
			InputStream st = JdbcTest.class.getResourceAsStream("/com/shiva/properties/dbproperties.properties");
			prop.load(st);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String url = prop.getProperty("dburl");
		String user = prop.getProperty("user");
		String password = prop.getProperty("password");


		String delQuery = " Delete from employees where last_name='Smith'";

		try (Connection myConn = DriverManager.getConnection(url,user,password);
				Statement myStmt = myConn.createStatement()){

			displayAllEmployees(myConn);
			System.out.println("Database connection successful!\n");


			/*System.out.println("Deleting..");
			myStmt.executeUpdate(delQuery);
			displayAllEmployees(myConn);*/
			selectByFirstName(myConn, "David");
			
			
			displaySalaryOfDepartment(myConn, "HR");
			System.out.println("--");
			increaseSalaryForDept(myConn, "HR", 300.0);
			displaySalaryOfDepartment(myConn, "HR");
			
			
			System.out.println(" -- ");
			greetTheDepartment(myConn,"Engineering");
			
			
			System.out.println("--");
			getCountOfDepartment(myConn, "HR");
			
			
			System.out.println("--");
			getEmployeesFromDepartment(myConn, "HR");
			
			getDBMetaData(myConn);
		}
		catch (Exception exc) {
			exc.printStackTrace();
		}

	}
	
	public static void displaySalaryOfDepartment(Connection con, String departName) {
		try(PreparedStatement myStmt = con.prepareStatement("select * from employees where department=?")){	
			myStmt.setString(1,departName);
			ResultSet myRs = myStmt.executeQuery();
			
			while (myRs.next()) {
				System.out.println(myRs.getString("last_name") + ", " + myRs.getString("first_name") + ", " + myRs.getDouble("salary"));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void displayAllEmployees(Connection con) {
		try(Statement myStmt = con.createStatement();
				ResultSet myRs = myStmt.executeQuery("select * from employees")){	

			while (myRs.next()) {
				System.out.println(myRs.getString("last_name") + ", " + myRs.getString("first_name"));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void selectByFirstName(Connection con, String name) {
		//Use Prepared Statement
		try(PreparedStatement myStmt = con.prepareStatement("select * from employees where first_name=?")){	
			myStmt.setString(1,name);
			ResultSet myRs = myStmt.executeQuery();
			
			while (myRs.next()) {
				System.out.println(myRs.getString("last_name") + ", " + myRs.getString("first_name"));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void increaseSalaryForDept(Connection con, String dept, Double hike) {
		//calling stored procedure
		try(CallableStatement cStmt = con.prepareCall("{call increase_salaries_for_department(?,?)}")){
			cStmt.setString(1, dept);
			cStmt.setDouble(2, hike);
			cStmt.execute();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void greetTheDepartment(Connection con, String dept) {
		//Using INOUT param
		try(CallableStatement cStmt = con.prepareCall("{call greet_the_department(?)}")){
			cStmt.setString(1, dept);
			cStmt.registerOutParameter(1, Types.VARCHAR);
			cStmt.execute();
			
			//Get the value of the INPUT parameter
			dept = cStmt.getString(1);
			
			System.out.println(dept);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void getCountOfDepartment(Connection con, String dept) {
		//Using OUT param
		try(CallableStatement cStmt = con.prepareCall("{call get_count_for_department(?,?)}")){
			cStmt.setString(1, dept);
			cStmt.registerOutParameter(2, Types.INTEGER);
			cStmt.execute();
			
			//Get the value of the INPUT parameter
			int count = cStmt.getInt(2);
			
			System.out.println(count);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void getEmployeesFromDepartment(Connection con, String dept) {
		//Using OUTPUT as ResultSet
		try(CallableStatement cStmt = con.prepareCall("{call get_employees_for_department(?)}")){
			cStmt.setString(1, dept);
			cStmt.execute();
			
			//Get the value of the INPUT parameter
			ResultSet rs = cStmt.getResultSet();
			
			while(rs.next()) {
				System.out.println(rs.getString("last_name") + ", " + rs.getString("first_name"));
			}
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void getDBMetaData(Connection con) {
		DatabaseMetaData metaData;
		try {
			metaData = con.getMetaData();
			System.out.println(metaData.getDatabaseProductName());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

}
