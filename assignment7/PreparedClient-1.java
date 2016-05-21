package edu.hu.cassandra;

import java.util.*;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
	
import com.datastax.driver.core.Statement;
	
	public class PreparedClient {
	   private Cluster cluster;
	   private Session session;

	   public void connect(String node) {
	      cluster = Cluster.builder()
	            .addContactPoint(node).build();
	      session = cluster.connect("mykeyspace");
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", 
	            metadata.getClusterName());
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	               host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	   }

	   public void createSchema() {
		   session.execute("DROP KEYSPACE IF EXISTS myKeySpaceTwo ;");
		   
		   session.execute("CREATE KEYSPACE myKeySpaceTwo WITH replication " + 
				      "= {'class':'SimpleStrategy', 'replication_factor':1};");

		   session.execute(
				"CREATE TABLE myKeySpaceTwo.person (" +
				"id uuid PRIMARY KEY," +
				"first text,"+
		   		"last text,"+
		   		"city text,"+ 
		   		"cell_1 text,"+ 
		   		"cell_2 text,"+
		   		"cell_3 text);");



	   }
	   public void loadData() {

	   		PreparedStatement statement = session.prepare(
	   			"INSERT INTO MyKeySpaceTwo.person (id, first, last, city, cell_1, cell_2, cell_3) " +
				    "VALUES (?, ?, ?, ?,?, ?, ?);");

	   		BoundStatement boundStatement = new BoundStatement(statement);

	   		session.execute(boundStatement.bind(
	   			UUID.randomUUID(), 
	   			"Ryan", 
	   			"Ballenger", 
	   			"Somerville", 
	   			"6144064942",
	   			null,
	   			null
	   		));
	   		
	   		session.execute(boundStatement.bind(
	   			UUID.randomUUID(), 
	   			"George", 
	   			"Washington", 
	   			"DC", 
	   			"6145556666", 
	   			null, 
	   			null
		   	));
	   		
	   		session.execute(boundStatement.bind(
	   			UUID.randomUUID(), 
	   			"Ice", 
	   			"Cube", 
	   			"Compton", 
	   			"6141112222", 
	   			null, 
	   			null
			 ));
	   		
	 

	   }
	   public void querySchema(){
		   ResultSet results = session.execute("SELECT * FROM myKeySpaceTwo.person;"); //+
	        //"WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		   System.out.println(String.format("%-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s \n%s", "First", "Last", "City", "Cell #1","Cell #2", "Cell #3","ID",
	    	  "--------------------------------------------------------------------------------------"));
		   for (Row row : results) {
			   System.out.println(String.format("%-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s |", row.getString("first"),
					   row.getString("last"),  row.getString("city"), row.getString("cell_1"),row.getString("cell_2"),row.getString("cell_3"), row.getUUID("id")));
		   }
		   System.out.println();
		   
	   }
	   
	   public void close() {
	      cluster.close(); // .shutdown();
	   }

	   public static void main(String[] args) {
	      PreparedClient client = new PreparedClient();
	      client.connect("127.0.0.1");
	      client.createSchema();
          client.loadData();
	      client.querySchema();
	      client.close();
	   }
	}
