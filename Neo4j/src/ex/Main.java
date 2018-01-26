package ex;

public class Main {

	public static void main(String[] args) {
		
		LabDatabase db = new LabDatabase();
		
		db.connect(7687,"","");
		
		db.emptyDatabase();
		db.fillDatabase();
		
		db.writeQueriesToFile("CBD_L44c_output.txt");

	}
}
