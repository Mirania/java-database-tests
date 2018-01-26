package ex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.*;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Indexes;

public class Commands {
	MongoClient mongo;
	MongoDatabase db;
	MongoCollection<Document> col;
	
	public Commands(String dbname, String colname) {
		mongo = new MongoClient();
		db = mongo.getDatabase(dbname);
		col = db.getCollection(colname);
		
		//silent mode
		Logger mongoLogger = Logger.getLogger( "org.mongodb" ); 
		mongoLogger.setLevel(Level.SEVERE);
	}
	
	//
	//
	//
	//
	// 2.02
	//
	//
	//
	//
	
	public void exercise(int number) throws ParseException {
		List<String> list = new ArrayList<>();
		FindIterable<Document> res = null;
		AggregateIterable<Document> ares = null;
		BasicDBObject query = new BasicDBObject();
		
		switch(number) {
		
		case 1: 
			res = col.find();
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 2:
			res = col.find()
            .projection(new Document("_id", 0)
                 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 3:
			res = col.find()
            .projection(new Document("_id", 0)
                 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("address.zipcode", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 4:
			query.put("localidade", "Bronx");
			res = col.find(query);
			for (Document doc : res)
				list.add(doc.toJson());
			System.out.println(list.size());
			break;
		case 5:
			query.put("localidade", "Bronx");
			res = col.find(query).limit(5);
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 6:
			query.put("localidade", "Bronx");
			res = col.find(query).skip(5).limit(5);
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 7:
			query.put("grades.score", new Document("$gt",85));
			res = col.find(query);
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 8:
			query.put("grades.score", new Document("$gt",85).append("$lte", 100));
			res = col.find(query);
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 9:
			query.put("address.coord.0",new Document("$lte",-95.7));
			res = col.find(query);
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 10:
			String[] s = {"American"};
			query.put("gastronomia",new Document("$nin", s));
			query.put("grades.score", new Document("$gt", 70));
			query.put("address.coord.0", new Document("$lt", -65));
			res = col.find(query);
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 11:
			query.put("nome", Pattern.compile("^Wil"));
			res = col.find(query)
            .projection(new Document("_id", 0)
                 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 12:
			query.put("nome", Pattern.compile("ces$"));
			res = col.find(query)
            .projection(new Document("_id", 0)
                 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 13:
			query.put("nome", Pattern.compile(".*Park.*"));
			res = col.find(query)
            .projection(new Document("_id", 0)
                 .append("nome", 1)
                 .append("localidade", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 14:
			String[] s2 = {"American", "Chinese"};
			query.put("localidade", "Bronx");
			query.put("gastronomia", new Document("$in", s2));
			res = col.find(query)
            .projection(new Document("_id", 0)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 15:
			String[] s3 = {"Staten Island", "Queens", "Bronx", "Brooklyn"};
			query.put("localidade", new Document("$in", s3));
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 16:
			String[] s4 = {"Staten Island", "Queens", "Bronx", "Brooklyn"};
			query.put("localidade", new Document("$nin", s4));
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 17:
			query.put("grades.score", new Document("$lte", 10));
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1)
                 .append("grades.score", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 18:
			String[] s5 = {"American", "Chinese"};
			Document[] s6 = {new Document("gastronomia", new Document("$in", s5)),
					new Document("nome", Pattern.compile("^Wil"))};
			query.put("$or", s6);
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("localidade", 1)
                 .append("gastronomia", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 19:
			long d = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse("2014-08-11T00:00:00Z").getTime();
			BasicDBObject s7 = new BasicDBObject("score",10)
			.append("grade", "A")
			.append("date", d);			
			BasicDBObject s8 = new BasicDBObject("$elemMatch", s7);
			query.put("grades", s8);
			res = col.find(query)
            .projection(new Document("_id", 0)
                 .append("nome", 1)
                 .append("grades", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 20:
			long d2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse("2014-08-11T00:00:00Z").getTime();
			Document[] s9 = {new Document("grades.1.grade", "A"),
					new Document("grades.1.date", d2)};
			query.put("$and", s9);
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("grades.score", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 21:
			query.put("address.coord.1", new Document("$gt",42).append("$lte", 52));
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("address", 1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 22:
			res = col.find()
            .projection(new Document("_id", 0)
                 .append("nome", 1)).sort(new Document("nome",1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 23:
			res = col.find()
            .projection(new Document("_id", 0)
                 .append("nome", 1)).sort(new Document("nome",-1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 24:
			res = col.find()
            .projection(new Document("_id", 0)
                 .append("nome", 1)
                 .append("gastronomia", 1)
                 .append("localidade", 1)).sort(new Document("gastronomia",1)
                		 .append("localidade", -1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 25:
			String[] s10 = {"American"};
			query.put("localidade", "Brooklyn");
			query.put("gastronomia", new Document("$nin", s10));
			query.put("grades.grade", "A");
			res = col.find(query)
            .projection(new Document("_id", 0)
            	 .append("restaurant_id", 1)
                 .append("nome", 1)
                 .append("gastronomia", 1)
                 .append("localidade", 1)
                 .append("grades", 1)).sort(new Document("gastronomia",-1));
			for (Document doc : res)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 26:
			query.put("address.rua", new Document("$not",Pattern.compile(".*")));
			res = col.find(query);
			for (Document doc : res)
				list.add(doc.toJson());
			System.out.println(list.size());
			break;
		case 27:
			ares = col.aggregate(Arrays.asList(
		            new Document("$group", new Document("_id", "$localidade").append("count", new Document("$sum", 1)))));
			for (Document doc : ares)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 28:
			ares = col.aggregate(Arrays.asList(
		            new Document("$unwind", "$grades"),
		            new Document("$group", new Document("_id", "$nome").append("sum", new Document("$sum", "$grades.score"))),
		            new Document("$match", new Document("sum", new Document("$gt", 85)))));
			for (Document doc : ares)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 29:
			ares = col.aggregate(Arrays.asList(
		            new Document("$unwind", "$grades"),
		            new Document("$group", new Document("_id", "$nome").append("sum", new Document("$sum", "$grades.score"))),
		            new Document("$match", new Document("sum", new Document("$gt", 80).append("$lte", 100)))));
			for (Document doc : ares)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 30:
			ArrayList<String> s11 = new ArrayList<>();
			s11.add("American");
			ares = col.aggregate(Arrays.asList(
		            new Document("$match", new Document("gastronomia", new Document("$nin", s11))),
		            new Document("$match", new Document("address.coord.0", new Document("$lt", -65))),
		            new Document("$unwind", "$grades"),
		            new Document("$group", new Document("_id", "$nome")
		            		.append("sum", new Document("$sum", "$grades.score"))),
		            new Document("$match", new Document("sum", new Document("$gt",80).append("$lte", 100)))));
			for (Document doc : ares)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;
		case 31:
			ares = col.aggregate(Arrays.asList(
		            new Document("$group", new Document("_id", "$gastronomia")
		            		.append("count", new Document("$sum",1)))));
			for (Document doc : ares)
				list.add(doc.toJson());
			for (String l: list)
				System.out.println(l);
			break;	
		default:
			System.out.println("Exercise does not exist.");
			
		}
		
	}
	//
	//
	//
	//
	// 2.04
	//
	//
	//
	//
	
	public void insert(String field, String content) {
		Document doc = new Document();
		doc.put(field, content);
		
		col.insertOne(doc);
	}
	
	public void edit(String name, String field, String content) {
		Document doc = new Document();
		doc.append("$set", new Document(field, content));

		BasicDBObject query = new BasicDBObject().append("nome", name);

		col.updateOne(query, doc);
	}
	
	public List<String> search(String field, String content) {
		List<String> list = new ArrayList<>();
		BasicDBObject query = new BasicDBObject();
		query.put(field, content);
		
		FindIterable<Document> res = col.find(query);
		for (Document doc : res)
			list.add(doc.toJson());
		return list;
	}
	
	public String indexesTestQueries() {
		long start = System.currentTimeMillis();
		
		BasicDBObject query = new BasicDBObject();
		query.put("localidade", "Bronx");
		col.find(query);
		
		query = new BasicDBObject();
		query.put("nome", "New Rainbow Restaurant");
		col.find(query);
		
		query = new BasicDBObject();
		query.put("gastronomia", "American");
		col.find(query);
		
		long end = System.currentTimeMillis();

		return String.format("Finished after %.4f seconds.",(end-start)/1000.0);
	}
	
	public void createIndexes() {
		col.createIndex(Indexes.descending("localidade"));
		col.createIndex(Indexes.descending("gastronomia"));
		col.createIndex(Indexes.descending("nome"));
	}
	
	@SuppressWarnings("unused")
	public int countLocalidades() {
		AggregateIterable<Document> res = col.aggregate(Arrays.asList(
	            new Document("$group", new Document("_id", "$localidade").append("count", new Document("$sum", 1)))));
		int size = 0;
		for (Document doc : res)
			size++;
		return size;
	}
	
	public Map<String, Integer> countRestByLocalidade() {
		Map<String, Integer> map = new HashMap<>();
		AggregateIterable<Document> res = col.aggregate(Arrays.asList(
	            new Document("$group", new Document("_id", "$localidade").append("count", new Document("$sum", 1)))));
		for (Document doc : res)
			map.put(doc.getString("_id"), doc.getInteger("count"));
		return map;
	}
	
	public Map<String, Integer> countRestByLocalidadeByGastronomia() {
		Map<String, Integer> map = new HashMap<>();
		AggregateIterable<Document> res = col.aggregate(Arrays.asList(
	            new Document("$group", new Document("_id", new Document("loc", "$localidade").append("gast", "$gastronomia"))
	            		.append("count", new Document("$sum", 1)))));
		for (Document doc : res) {
			Document id = (Document) doc.get("_id");
			map.put(id.getString("loc")+" | "+id.getString("gast"), doc.getInteger("count"));
		}
		return map;
	}
	
	public List<String>	getRestWithNameCloserTo(String name) {
		List<String> list = new ArrayList<>();
		BasicDBObject query = new BasicDBObject();
		query.put("nome", Pattern.compile(name));
		FindIterable<Document> res = col.find(query);
		for (Document doc : res)
			list.add(doc.getString("nome"));
		return list;
	}
}