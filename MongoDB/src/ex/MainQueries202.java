package ex;

import java.text.ParseException;

public class MainQueries202 {

	public static void main(String[] args) throws ParseException {
		
		Commands c = new Commands("test", "restaurants");
		
		for (int i=1;i<=31;i++) {
			c.exercise(i);
			System.out.println(i+" finished.\n");
		}
		
		
	}
}
