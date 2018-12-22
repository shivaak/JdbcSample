package com.shiva;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class JdbcBlobDataSample {

	public static void main(String[] args) {
		
		InputStream is = JdbcBlobDataSample.class.getResourceAsStream("/com/shiva/properties/dbproperties.properties");
		Properties props = new Properties();
		try {
			props.load(is);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String url = props.getProperty("dburl");
		String user = props.getProperty("user");
		String password = props.getProperty("password");

		

		try(Connection con = DriverManager.getConnection(url, user, password)){
			writeBlob(con);
			readBlob(con);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void writeBlob(Connection con) {
		String sql = "Update employees set resume=? where first_name='John' and last_name='Doe'";
		
		try(PreparedStatement st = con.prepareStatement(sql);){
			st.setBinaryStream(1, new FileInputStream("sampleresume.doc"));
			st.executeUpdate();
			System.out.println("File Uploaded..!");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void readBlob(Connection con) {
		String sql = "select resume from employees where first_name='John' and last_name='Doe'";
		
		try(Statement st = con.createStatement();
			ResultSet rs = st.executeQuery(sql);){
			
			File f = new File("John_Resume.doc");
			FileOutputStream fos = new FileOutputStream(f);
			
			if(rs.next()) {
				InputStream bs = rs.getBinaryStream("resume");
				byte[] buffer = new byte[1024];
				while(bs.read(buffer) > 0) {
					fos.write(buffer);
				}
				bs.close();
			}
			fos.close();
			System.out.println("File Downloaded to : " + f.getAbsolutePath());
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
