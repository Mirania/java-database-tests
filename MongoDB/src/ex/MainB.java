package ex;

public class MainB {

	public static void main(String[] args) {
		Commands c = new Commands("test", "restaurants");
		
		System.out.println(c.indexesTestQueries());
		c.createIndexes();
		System.out.println(c.indexesTestQueries());
	}
}
