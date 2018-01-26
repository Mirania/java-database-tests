package ex;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class LabDatabase {
	
	Driver driver;
	Session session;
	
	//base de dados Northwind
	//dados retirados do website:
	//https://gist.github.com/adam-cowley/79923b538f6851d30e08
	private static String CUSTOMERS = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/customers.csv\"";
	private static String PRODUCTS = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/products.csv\"";
	private static String SUPPLIERS = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/suppliers.csv\"";
	private static String EMPLOYEES = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/employees.csv\"";
	private static String CATEGORIES = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/categories.csv\"";
	private static String ORDERS = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/orders.csv\"";
	private static String ORDER_DETAILS = "\"https://raw.githubusercontent.com/adam-cowley/northwind-neo4j/master/data/order-details.csv\"";
	
	public void connect(int boltport, String user, String pword) {
		driver = GraphDatabase.driver( "bolt://localhost:"+boltport, AuthTokens.basic(user,pword) );
		session = driver.session();
	}
	
	public void writeQueriesToFile(String filename) {
		try {
			File f = new File(filename);
			if (!f.exists()) f.createNewFile();
			
			PrintWriter writer = new PrintWriter(f, "UTF-8");
			for (int i=1;i<=10;i++) writer.write(query(i)+"\n");
			writer.close();
			
			System.out.println("Ficheiro criado.");
			
		} catch (IOException e) {
			System.out.println("Erro a guardar queries no ficheiro.");
		}
	}
	
	public String query(int i) {
		String s = "("+i+") ";
		
		switch (i) {
		case 1:
			s += "Os 5 produtos mais caros:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (p:Product) "
	                				+ "return p.productName,p.unitPrice order by p.unitPrice desc limit 5"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	String price = String.format("%.2f€", r.get(1).asDouble());
	                	res += r.get(0).asString()+" tem um preço de "+price+".\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 2:
			s += "Lista de managers e quantidade de empregados:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (m:Employee)<-[r:REPORTS_TO]-(:Employee) "
	                				+ "return m.firstName,m.lastName,count(r)"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	res += r.get(0).asString()+" "+r.get(1).asString()+" tem "+r.get(2).asInt()+ " empregado";
	                	if (r.get(2).asInt()>1) res += "s.\n";
	                	else res += ".\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 3:
			s += "Quantos produtos diferentes são fornecidos por Exotic Liquids:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (s:Supplier {companyName:\"Exotic Liquids\"})-[r:SUPPLIES]->(:Product) "
	                				+ "return count(r)"
	                		);
	                String res = "";
	                res += "Exotic Liquids fornece "+sr.single().get(0).asInt()+" tipos de produtos.\n";
	                return res;
	            }
	        } );
			break;
		case 4:
			s += "Distância entre o produto Chai e o produto Geitost:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match path=shortestPath((x:Product {productName:\"Chai\"})-[*]-(n:Product {productName:\"Geitost\"})) "
	                				+ "return length(path)"
	                		);
	                String res = "";
	                res += "O caminho mais curto tem tamanho "+sr.single().get(0).asInt()+".\n";
	                return res;
	            }
	        } );
			break;
		case 5:
			s += "Produtos mais comprados:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (:Customer)-[:PURCHASED]->(o:Order)-[:PRODUCT]->(p:Product) "
	                				+ "with p,count(o) as o "
	                				+ "return p.productName,o order by o desc limit 10"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	res += "O produto "+r.get(0).asString()+" foi comprado "+r.get(1).asInt()+ "x.\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 6:
			s += "Companhias que menos investiram:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (c:Customer)-[:PURCHASED]->(o:Order)-[:PRODUCT]->(:Product) "
	                				+ "with c,count(o) as o "
	                				+ "return c.companyName,o order by o limit 10"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	res += "A companhia "+r.get(0).asString()+" comprou um total de "+r.get(1).asInt()+ " produtos.\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 7:
			s += "Companhias que mais dinheiro gastaram:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (c:Customer)-[:PURCHASED]->(o:Order)-[:PRODUCT]->(p:Product) "
	                				+ "with c,sum(p.unitPrice) as total "
	                				+ "return c.companyName,total order by total desc limit 5"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	String price = String.format("%.2f€", r.get(1).asDouble());
	                	res += "A companhia "+r.get(0).asString()+" gastou "+price+ " em produtos.\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 8:
			s += "Total de gastos guardados na base de dados:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (c:Customer)-[:PURCHASED]->(o:Order)-[:PRODUCT]->(p:Product) "
	                				+ "with c,sum(p.unitPrice) as total "
	                				+ "with sum(total) as final "
	                				+ "return final"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	String price = String.format("%.2f€", r.get(0).asDouble());
	                	res += "O total é de "+price+".\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 9:
			s += "Número médio de pedidos satisfeitos:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (e:Employee)-[r:SOLD]->(o:Order) "
	                				+ "with e,count(r) as c "
	                				+ "with avg(c) as a "
	                				+ "return a"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	String round = String.format("%.2f", r.get(0).asDouble());
	                	res += "A média é de "+round+".\n";
	                }
	                return res;
	            }
	        } );
			break;
		case 10:
			s += "Produtos por categoria, e o seu preço total:\n";
			s += session.writeTransaction( new TransactionWork<String>() {
	            @Override public String execute( Transaction tx ) {
	                StatementResult sr =
	                		tx.run( "match (p:Product)-[r:PART_OF]->(c:Category) "
	                				+ "with c,count(r) as amount,sum(p.unitPrice) as total "
	                				+ "return c.categoryName,amount,total"
	                		);
	                String res = "";
	                while (sr.hasNext()) {
	                	Record r = sr.next();
	                	String price = String.format("%.2f€", r.get(2).asDouble());
	                	res += "A categoria "+r.get(0).asString()+" tem "+r.get(1).asInt()+" produtos, com preço total de "+price+".\n";
	                }
	                return res;
	            }
	        } );
			break;
		
		
		}				
		return s;
	}
	
	public void fillDatabase() {
		System.out.println("A adicionar dados...");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+CUSTOMERS+" AS row "
                		+ "CREATE (:Customer {companyName: row.companyName, customerID: row.customerID, fax: row.fax, phone: row.phone});"
                		);
                return "Done";
            }
        } );	
		System.out.print("1/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+PRODUCTS+" AS row "
                		+ "CREATE (:Product {productName: row.productName, productID: row.productID, unitPrice: toFloat(row.unitPrice)});"
                		);
                return "Done";
            }
        } );
		System.out.print("2/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+SUPPLIERS+" AS row "
                		+ "CREATE (:Supplier {companyName: row.companyName, supplierID: row.supplierID});"
                		);
                return "Done";
            }
        } );
		System.out.print("3/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+EMPLOYEES+" AS row "
                		+ "CREATE (:Employee {employeeID:row.employeeID,  firstName: row.firstName, lastName: row.lastName, title: row.title});"
                		);
                return "Done";
            }
        } );
		System.out.print("4/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+CATEGORIES+" AS row "
                		+ "CREATE (:Category {categoryID: row.categoryID, categoryName: row.categoryName, description: row.description});"
                		);
                return "Done";
            }
        } );
		System.out.print("5/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+ORDERS+" AS row "
                		+ "MERGE (order:Order {orderID: row.orderID}) ON CREATE SET order.shipName =  row.shipName;"
                		);
                return "Done";
            }
        } );
		System.out.print("6/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+ORDER_DETAILS+" AS row "
                		+ "MATCH (order:Order {orderID: row.orderID}) "
                		+ "MATCH (product:Product {productID: row.productID}) "
                		+ "MERGE (order)-[pu:PRODUCT]->(product) "
                		+ "ON CREATE SET pu.unitPrice = toFloat(row.unitPrice), pu.quantity = toFloat(row.quantity);"
                		);
                return "Done";
            }
        } );
		System.out.print("7/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+ORDERS+" AS row "
                		+ "MATCH (order:Order {orderID: row.orderID}) "
                		+ "MATCH (employee:Employee {employeeID: row.employeeID}) "
                		+ "MERGE (employee)-[:SOLD]->(order);"
                		);
                return "Done";
            }
        } );
		System.out.print("8/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+ORDERS+" AS row "
                		+ "MATCH (order:Order {orderID: row.orderID}) "
                		+ "MATCH (customer:Customer {customerID: row.customerID}) "
                		+ "MERGE (customer)-[:PURCHASED]->(order);"
                		);
                return "Done";
            }
        } );
		System.out.print("9/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+PRODUCTS+" AS row "
                		+ "MATCH (product:Product {productID: row.productID}) "
                		+ "MATCH (supplier:Supplier {supplierID: row.supplierID}) "
                		+ "MERGE (supplier)-[:SUPPLIES]->(product);"
                		);
                return "Done";
            }
        } );
		System.out.print("10/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+PRODUCTS+" AS row "
                		+ "MATCH (product:Product {productID: row.productID}) "
                		+ "MATCH (category:Category {categoryID: row.categoryID}) "
                		+ "MERGE (product)-[:PART_OF]->(category);"
                		);
                return "Done";
            }
        } );
		System.out.print("11/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+EMPLOYEES+" AS row "
                		+ "MATCH (employee:Employee {employeeID: row.employeeID}) "
                		+ "MATCH (manager:Employee {employeeID: row.reportsTo}) "
                		+ "MERGE (employee)-[:REPORTS_TO]->(manager);"
                		);
                return "Done";
            }
        } );
		System.out.print("12/13 ");
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "LOAD CSV WITH HEADERS FROM "+ORDER_DETAILS+" AS row "
                		+ "MATCH (product:Product {productID: row.productID}) "
                		+ "MERGE (order)-[pu:PRODUCT]->(product) "
                		+ "ON CREATE SET pu.unitPrice = toFloat(row.unitPrice), pu.quantity = toFloat(row.quantity);"
                		);
                return "Done";
            }
        } );
		System.out.println("13/13");
        System.out.println("Terminado.");
	}
	
	public void emptyDatabase() {
		session.writeTransaction( new TransactionWork<String>() {
            @Override public String execute( Transaction tx ) {
                tx.run( "match (n)"
                		+ "detach delete n;"
                		);
                return "Done";
            }
        } );
        System.out.println("Base de dados foi limpa.");
	}
}
