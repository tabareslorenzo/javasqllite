package m3challenge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;

public class CSVToDb {
	public static void main(String[] args) throws FileNotFoundException, IOException 
	{
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter filename");
		String filename = scan.next();
		System.out.println(filename);
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("resources/" + filename + ".csv"))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(",");
		        records.add(Arrays.asList(values));
		        
		    }
		}
		int failed = 0;
		int success = 0;
		List<List<String>> failedList = new ArrayList<>();;
		List<List<String>> successList = new ArrayList<>();;
		for(List<String> str : records)
		{
			boolean fail = false;
			if(str.size() > 10)
			{
				fail = true;
			}
			for(int i = 0; i < str.size(); i++)
			{
//	
//				
			}
			if(fail)
			{
				failedList.add(str);
				failed++;
			}
			else
			{
				successList.add(str);
				success++;
			}
		}
		System.out.println("Failed : " + failed);
		System.out.println("Success : " + success);
		//System.out.println(failedList);
		createLog(failed, success, filename);
		createCsv(failedList, filename);
		createDb(successList, filename);
	}
	public static void createCsv(List<List<String>> list, String filename)
	{
		try {
			   PrintWriter pw= new PrintWriter(new File("resources/" + filename + "-bad.csv"));
			   StringBuilder sb=new StringBuilder();
			   sb.append("A");
			   sb.append(",");
			   sb.append("B");
			   sb.append(",");
			   sb.append("C");
			   sb.append(",");
			   sb.append("D");
			   sb.append(",");
			   sb.append("E");
			   sb.append(",");
			   sb.append("F");
			   sb.append(",");
			   sb.append("G");
			   sb.append(",");
			   sb.append("H");
			   sb.append(",");
			   sb.append("I");
			   sb.append(",");
			   sb.append("J");
			   sb.append("\r\n");
			   for(List<String> str : list)
			   {
				   for(int i = 0; i < str.size(); i++)
				   {
					   sb.append(str.get(i));
					   if(i == str.size()-1)
					   {
						   sb.append("\r\n");
					   }
					   else
					   {
						   sb.append(",");
					   }
				   }
			   }
			   pw.write(sb.toString());
			   pw.close();
			   
			   } catch (Exception e) {
			      // TODO: handle exception
			   }
	}
	public static void createNewTable(String filename) {
        // SQLite connection string
        String url = "jdbc:sqlite:" + filename + ".db";
        
        String sql = "CREATE TABLE logs (\n"
                + "	A text NOT NULL,\n"
                + "	B text NOT NULL,\n"
                + "	C text NOT NULL,\n"
                + "	D text NOT NULL,\n"
                + "	E text NOT NULL,\n"
                + "	F text NOT NULL,\n"
                + "	G text NOT NULL,\n"
                + "	H text NOT NULL,\n"
                + "	I text NOT NULL,\n"
                + "	J text NOT NULL\n"
                + ");";
        
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
        	DatabaseMetaData meta = conn.getMetaData();
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
	public static void insert(List<String> list) {
		String url = "jdbc:sqlite:tests.db";


        String sql = "INSERT INTO logs VALUES(?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = DriverManager.getConnection(url);
        		 PreparedStatement pstmt = conn.prepareStatement(sql)) {
        	for(int i = 0; i < list.size(); i++)
        	{
        		
        		pstmt.setString(i+1, list.get(i));
        	}
        	
            //pstmt.setDouble(2, capacity);
            pstmt.executeUpdate();
            // create a new table
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


    }
	public static void createDb(List<List<String>> list, String filename)
	{
		createNewTable(filename);
		for(List<String> str : list)
		{
			insert(str);
		}
	}
	public static void createLog(int failed, int success, String filename)
	{
		Logger logger = Logger.getLogger("MyLog");  
	    FileHandler fh;  

	    try {  

	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler("resources/" + filename + ".log");  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter);  

	        // the following statement is used to log any messages  
	        logger.info("total: " + (failed + success));
	        logger.info("failed:  " + failed);
	        logger.info("success: " + success);

	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }  
 
		
	}

}
