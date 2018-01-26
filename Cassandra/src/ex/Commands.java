package ex;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Commands {
	private Cluster cluster; 
    private Session session;
 
    public Commands(String node, int port) {
        cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        session = cluster.connect();
        this.fillDatabase();
    }
    
    public void fillDatabase() {
    	session.execute("CREATE KEYSPACE IF NOT EXISTS "+
    					"ks WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}");
    	session.execute("CREATE TABLE IF NOT EXISTS ks.tab("+
    					"key text, "+
    					"value int, "+
    					"objects list<text>, "+
    					"PRIMARY KEY(key));");
    	session.execute("INSERT INTO ks.tab JSON '{\"key\":\"a\",\"value\":175,\"objects\":[\"x\",\"y\"]}'");
    	session.execute("INSERT INTO ks.tab JSON '{\"key\":\"b\",\"value\":185,\"objects\":[\"z\"]}'");
    	session.execute("INSERT INTO ks.tab JSON '{\"key\":\"c\",\"value\":12,\"objects\":[\"zz\"]}'");
    	session.execute("INSERT INTO ks.tab JSON '{\"key\":\"d\",\"value\":15,\"objects\":[\"abc\"]}'");
    	session.execute("INSERT INTO ks.tab JSON '{\"key\":\"e\",\"value\":175,\"objects\":[\"g\",\"h\"]}'");
    }
    
    public void insert(String key, Integer value, ArrayList<String> objs) {
    	String s = "INSERT INTO ks.tab JSON '{\"key\":\""+key+"\"";
    	if (value!=null) {
    		s += ",\"value\":"+value;
    	}
    	if (objs.size()>0) {
    		s += ",\"objects\":[";
    		for (int i=0;i<objs.size()-1;i++) {
    			s += "\""+objs.get(i)+"\",";
    		}
    		s += "\""+objs.get(objs.size()-1)+"\"]";
    	}
    	s += "}'";
    	session.execute(s);
    }
    
    public void edit(String key, int value) {
    	session.execute("UPDATE ks.tab SET value = "+value+" WHERE key = '"+key+"' IF EXISTS");
    }
    
    public void search(String key) {
    	ResultSet set = session.execute("SELECT * FROM ks.tab WHERE key ='"+key+"'");
    	List<Row> rows = set.all();
    	for (Row r: rows)
    		System.out.println("Key: "+r.getString("key")+", Value: "+r.getInt("value")+", Objects: "+r.getList("objects", String.class));
    }
    
    public void check() {
    	ResultSet set = session.execute("SELECT * FROM ks.tab");
    	List<Row> rows = set.all();
    	for (Row r: rows)
    		System.out.println("Key: "+r.getString("key")+", Value: "+r.getInt("value")+", Objects: "+r.getList("objects", String.class));
    }
 
    public void close() {
        session.close();
        cluster.close();
    }
}
