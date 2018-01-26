package ex;

import java.util.List;
import java.util.Map;

public class MainC {

	public static void main(String[] args) {
		Commands c = new Commands("test", "restaurants");
		System.out.println("Numero de localidades distintas: "+c.countLocalidades());
		
		System.out.println("Numero de restaurantes por localidade:");
		Map<String, Integer> map = c.countRestByLocalidade();
		for (Map.Entry<String, Integer> entry : map.entrySet())
			System.out.println("-> "+entry.getKey()+" - "+entry.getValue());
		
		System.out.println("Numero de restaurantes por localidade e gastronomia:");
		Map<String, Integer> map2 = c.countRestByLocalidadeByGastronomia();
		for (Map.Entry<String, Integer> entry : map2.entrySet())
			System.out.println("-> "+entry.getKey()+" - "+entry.getValue());
		
		System.out.println("Nome de restaurantes contendo 'Park' no nome:");
		List<String> list = c.getRestWithNameCloserTo("Park");
		for (String s : list)
			System.out.println("-> "+s);
	}

}
