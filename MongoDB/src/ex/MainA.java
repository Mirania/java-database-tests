package ex;

import java.util.Scanner;

public class MainA {

	public static void main(String[] args) {
		Commands c = new Commands("test", "restaurants");
		Scanner sc = new Scanner(System.in);
		boolean go = true;
		
		while (go) {
			System.out.println("(1) Insert");
			System.out.println("(2) Edit");
			System.out.println("(3) Search");
			System.out.println("(4) Exit");
			System.out.print("Option: ");
			String op = sc.next();
			
			switch (op) {
			case "1":
				System.out.print("\nField: ");
				String field = sc.next();
				System.out.print("Content: ");
				c.insert(field, sc.next());
				break;
			case "2":
				System.out.print("\nName of entry to edit: ");
				String name = sc.next();
				System.out.print("Field: ");
				field = sc.next();
				System.out.print("Content: ");
				c.edit(name, field, sc.next());
				break;
			case "3":
				System.out.print("\nField: ");
				field = sc.next();
				System.out.print("Must equal: ");
				for (String s: c.search(field, sc.next()))
					System.out.println(s);
				break;
			case "4":
				go = false;
				break;
			default:
				System.out.println("\nInvalid option.");
				break;
			}
			
			System.out.println();
			
		}
		
		sc.close();
	}
}
