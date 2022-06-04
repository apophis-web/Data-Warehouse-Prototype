package test;
import java.sql.*;
import java.util.*;
import org.apache.commons.collections4.multimap.*;
import org.apache.commons.collections4.*;
import java.util.concurrent.ArrayBlockingQueue;

public class SQL 
{
	public static void MULTIVALUEDMAP(ResultSet transaction_data, ArrayBlockingQueue<List<Map<String,String>>> array_blocking_queue, MultiValuedMap <String,Map<String,String>> multi_map, List <Map<String, String>> list1) throws SQLException
	{
	    for (int x = 0; transaction_data.next() != false; x++)
	    {
	 	   Map<String, String> transaction_map = new HashMap<String, String>();
	 	   
		 	try {
		 	   String transaction_id = transaction_data.getString("TRANSACTION_ID");
		 	   transaction_map.put("TRANSACTION_ID", transaction_id);
		 	  	} catch (Exception e){}
		 	  
		 	
		 	try {
		 	   String product_id = transaction_data.getString("PRODUCT_ID");
		 	   transaction_map.put("PRODUCT_ID", product_id);
		 	 } catch (Exception e){}
		 	 
		 	 
		 	try {
		 	   String customer_id = transaction_data.getString("CUSTOMER_ID");
		 	   transaction_map.put("CUSTOMER_ID", customer_id);
		 	 } catch (Exception e){}
		 	   
		 	
		 	try {
		 	   String customer_name = transaction_data.getString("CUSTOMER_NAME");
		 	   transaction_map.put("CUSTOMER_NAME", customer_name);
		 	} catch (Exception e){}
		 	
		 	
		 	try {
		 	   String store_id = transaction_data.getString("STORE_ID");
		 	   transaction_map.put("STORE_ID", store_id);
		 	} catch (Exception e){}
		 	
		 	
		 	try {
		 	   String tdate = transaction_data.getString("T_DATE");
		 	   transaction_map.put("T_DATE", tdate);
		 	} catch (Exception e){} 
		 	
		 	
		 	try {
		 	   String store_name = transaction_data.getString("STORE_NAME");
		 	   transaction_map.put("STORE_NAME", store_name);
		 	} catch (Exception e){} 
		 	
		 	
		 	try {
		 	   String quantity = transaction_data.getString("QUANTITY");
		 	   transaction_map.put("QUANTITY", quantity);   
		 	} catch (Exception e){} 
	 	   
	 	   list1.add(transaction_map);
	 	   multi_map.put(transaction_map.get("PRODUCT_ID"), transaction_map);
	 	
	    }
	    
	    if (array_blocking_queue.size() >= 10)
	    {
	        for(Map<String,String> MAP: array_blocking_queue.poll())
	     	   multi_map.removeMapping(MAP.get("PRODUCT_ID"), MAP);
	    }
	    
	    array_blocking_queue.add(list1);
	}
	
	public static int INSERTION(ResultSet master_data, Connection connection,  MultiValuedMap <String,Map<String,String>> multi_map, int count) throws SQLException
	{
		Statement stmt3 = connection.createStatement();
		for (int x = 0; master_data.next() != false; x++)
        {
     	   for (Map<String, String> i: multi_map.get(master_data.getString("PRODUCT_ID")))
     	   {
     		   	   String master_supplier_id = master_data.getString("SUPPLIER_ID");
	        	   String master_product_name = master_data.getString("PRODUCT_NAME");
	        	   String master_supplier_name = master_data.getString("SUPPLIER_NAME");
	        	   String master_product_id = master_data.getString("PRODUCT_ID");
	        	   float master_price = master_data.getFloat("PRICE");
	        	   
	        	   float transaction_quantity = Integer.parseInt(i.get("QUANTITY"));
	        	   String product_id = i.get("PRODUCT_ID");
	        	   String tdate = i.get("T_DATE");
	        	   String customer_id = i.get("CUSTOMER_ID");
	        	   String customer_name = i.get("CUSTOMER_NAME");
	        	   String transaction_id = i.get("TRANSACTION_ID");
	        	   String store_id = i.get("STORE_ID");
	        	   String store_name = i.get("STORE_NAME");
	        	   count++;
	        	
	        	   System.out.println(master_price * transaction_quantity);
	        	   
	        	   try {
	        		   String customer_insertion = "INSERT into customer (CUSTOMER_ID, CUSTOMER_NAME) values ('" + customer_id + "', '" + customer_name + "')";
	        		   stmt3.executeUpdate(customer_insertion);
	        	   } catch (Exception e){}
	        	   
	        	   
	        	   try {
	        		   String customer_insertion = "INSERT into product (PRODUCT_ID, PRODUCT_NAME, PRICE) values ('" + master_product_id + "', '" + master_product_name + "', '" + master_price + "')";
	        		   stmt3.executeUpdate(customer_insertion);
	        	   } catch (Exception e){}
	        	   
	        	   
	        	   try {
	        		   String master_supplier_name2 = master_supplier_name.replace("'", "");
	        		   String customer_insertion = "INSERT into supplier (SUPPLIER_ID, SUPPLIER_NAME) values ('" + master_supplier_id + "', '" + master_supplier_name2 + "')";
	        		   stmt3.executeUpdate(customer_insertion);
	        	   } catch (Exception e){}
	        	   
	        	   
	        	   try {
	        		   String customer_insertion = "INSERT into store (STORE_ID, STORE_NAME) values ('" + store_id + "', '" + store_name + "')";
	        		   stmt3.executeUpdate(customer_insertion);
	        	   } catch (Exception e){}
	        	      
	        	   
	        	   try {
	        		   String[] parts = tdate.split("-");
	        		   String year = parts[0];
	        		   String month = parts[1];
	        		   String day = parts[2];  
	        		   int quarter = (Integer.parseInt(month)/4) + 1;
	        		   String customer_insertion = "INSERT into date_ (datee, day_, month_, quarter_) values ('" + tdate + "', '" + day + "', '" + month + "', '" + quarter + "')";
	        		   stmt3.executeUpdate(customer_insertion);
	        	   } catch (Exception e){}
	        	   
	        	   
	        	   try {
	        		   String customer_insertion = "INSERT into facttable (sales, quantity, FK_PRODUCT_ID, FK_SUPPLIER_ID, FK_date, FK_STORE_ID, FK_CUSTOMER_ID) values ('" + (master_price * transaction_quantity) + "', '" + transaction_quantity + "', '" + master_product_id + "', '" + master_supplier_id + "', '" + tdate + "', '" + store_id + "', '" + customer_id + "')";
	        		   stmt3.executeUpdate(customer_insertion);
	        	   } catch (Exception e) {System.out.println(e);}
     	   }
        }
		return count;
	}
	
	public static void main(String[] args) throws SQLException 
	{
		int count = 0;
		   //Establishing connection
		   Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/dwh", "root", "1234"); 
		   System.out.println("Connected With the database successfully");
		   
		   int count1 = 0;
		   int count2 = 0;
		   MultiValuedMap <String,Map<String,String>> multi_map = new ArrayListValuedHashMap<>();
           ArrayBlockingQueue<List<Map<String,String>>> array_blocking_queue = new ArrayBlockingQueue<List<Map<String,String>>>(10);
           int condition = 0;
		   while(condition == 0) 
		   {
			   if (count == 10000)
				   condition++;
			   
			   if (count2 == 100)
				   count2 = 0;
			   
			   Statement stmt = connection.createStatement(); 	//Query execution statement
			   Statement stmt2 = connection.createStatement();	 //Query execution statement
			   String str1 = "select * from masterdata limit " + count2 + ",10";
			   String str2 = "select * from transactions limit " + count1 + ",50";
			   
	           ResultSet transaction_data = stmt2.executeQuery(str2); 		   //Loading Data from schema
	           
	           List <Map<String, String>> list1 = new ArrayList<Map<String, String>>();
	              
	           ResultSet master_data = stmt.executeQuery(str1); 		   //Loading Data from schema
	           
	           MULTIVALUEDMAP(transaction_data,  array_blocking_queue, multi_map, list1);
	           count = INSERTION(master_data, connection, multi_map, count);        
	           count1 = count1 + 50;
	           count2 = count2 + 10;
		   }
		   System.out.println(count);
	}
}
