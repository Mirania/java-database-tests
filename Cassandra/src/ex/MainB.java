package ex;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class MainB {

	public static void main(String[] args) {
		
		Cluster cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build();
	    Session session = cluster.connect();
	    ResultSet set;
	    List<Row> rows;
	    
	    session.execute("USE vc");
	    
	    System.out.println("2.02.2");
	    set = session.execute("SELECT restaurant_id, nome, localidade, gastronomia FROM rest");
    	rows = set.all();
    	for (Row r: rows)
    		System.out.println(r);
    	
    	System.out.println();
    	System.out.println("2.02.4");
	    set = session.execute("SELECT count(*) FROM rest WHERE localidade='Bronx'");
    	rows = set.all();
    	for (Row r: rows)
    		System.out.println(r);
    	
    	System.out.println();
    	System.out.println("2.02.15");
	    set = session.execute("SELECT restaurant_id, nome, localidade, gastronomia "+
	    					"FROM rest WHERE localidade IN ('Staten Island', 'Queens', 'Bronx', 'Brooklyn')");
    	rows = set.all();
    	for (Row r: rows)
    		System.out.println(r);
    	
    	System.out.println();
    	System.out.println("2.02.19");
	    set = session.execute("SELECT * FROM gradesrest WHERE score=10 AND grade='A' AND date=1407715200");
    	rows = set.all();
    	for (Row r: rows)
    		System.out.println(r);
    	
    	System.out.println();
    	System.out.println("2.02.27");
	    set = session.execute("SELECT DISTINCT localidade from rest");
    	rows = set.all();
    	for (Row r: rows) {
    		String loc = r.getString("localidade");
    		ResultSet rs = session.execute("SELECT count(*) AS c FROM rest WHERE localidade='"+loc+"'");
    		Row row = rs.one();
    		long count = row.getLong("c");
    		System.out.println(loc+" - "+count);
    	}
		
	}
}
