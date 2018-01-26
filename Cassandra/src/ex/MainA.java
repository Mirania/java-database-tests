package ex;

import java.util.ArrayList;
import java.util.Scanner;

public class MainA {

	public static void main(String[] args) {
		Commands c = new Commands("localhost", 9042);
		Scanner sc = new Scanner(System.in);
		boolean go = true;
		
		while (go) {
			System.out.println("(1) Insert");
			System.out.println("(2) Edit");
			System.out.println("(3) Search");
			System.out.println("(4) Check content");
			System.out.println("(5) Exit");
			System.out.print("Option: ");
			String op = sc.next();
			
			switch (op) {
			case "1":
				try {
					System.out.print("\nKey: ");
					String key = sc.next();
					
					System.out.print("Value (\"none\" if no value): ");
					String val = sc.next();
					Integer v = null;
					if (!val.toLowerCase().equals("none"))
						v = Integer.parseInt(val);	

					ArrayList<String> obj = new ArrayList<String>();
					while (true) {
						System.out.print("Objects (\"none\" to stop): ");			
						String o = sc.next();
						if (o.toLowerCase().equals("none"))
							break;
						obj.add(o);
					}
					c.insert(key, v, obj);
				} catch (NumberFormatException e) {System.out.println("Value must be an integer.");}
				break;
			case "2":
				try {
					System.out.print("\nName of key to edit: ");
					String key = sc.next();
					System.out.print("New value: ");
					int v = Integer.parseInt(sc.next());
					c.edit(key, v);
				} catch (NumberFormatException e) {System.out.println("Value must be an integer.");}
				break;
			case "3":
				System.out.print("\nKey: ");
				String key = sc.next();
				c.search(key);
				break;
			case "4":
				System.out.println();
				c.check();
				break;	
			case "5":
				sc.close();
				System.exit(0);
			default:
				System.out.println("\nInvalid option.");
				break;
			}
			
			System.out.println();
			
		}
		
		
	}
}
