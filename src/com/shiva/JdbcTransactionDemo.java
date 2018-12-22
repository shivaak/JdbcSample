package com.shiva;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class JdbcTransactionDemo {
	public static void main(String[] args) {
	
		Properties prop = new Properties();
		try {
			InputStream st = JdbcTransactionDemo.class.getResourceAsStream("/com/shiva/properties/dbproperties.properties");
			prop.load(st);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String url = prop.getProperty("dburl");
		String user = prop.getProperty("user");
		String password = prop.getProperty("password");

		try (Connection myConn = DriverManager.getConnection(url,user,password);
				Statement myStmt = myConn.createStatement();
				Scanner in = new Scanner(System.in);){
			myConn.setAutoCommit(false);
			
			
			selectByLastName(myConn,  "Williams");
			selectByLastName(myConn,  "Adams");
			myStmt.executeUpdate("Update employees set first_name='Will' where first_name='Dravid'");
			myStmt.executeUpdate("Update employees set first_name='Smith' where first_name='Antony'");
			
			
			System.out.println("Do you wanna update (yes/no) : ");
			
			String opt = in.nextLine();
			
			switch(opt) {
			case "yes":
				myConn.commit();
				System.out.println("After Update : ");
				selectByLastName(myConn,  "Williams");
				selectByLastName(myConn,  "Adams");
				break;
			case "no":
				myConn.rollback();
				System.out.println("Update Rollbacked");
				break;
			}
			
			myStmt.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void selectByLastName(Connection con, String name) {
		//Use Prepared Statement
		try(PreparedStatement myStmt = con.prepareStatement("select * from employees where last_name=?")){	
			myStmt.setString(1,name);
			ResultSet myRs = myStmt.executeQuery();
			
			while (myRs.next()) {
				System.out.println(myRs.getString("last_name") + ", " + myRs.getString("first_name"));
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
