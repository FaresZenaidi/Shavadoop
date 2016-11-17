import java.util.*;

public class MapUtil
{
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map, int maxCounter)
	{
		List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
				{
			@Override
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				return (o2.getValue()).compareTo(o1.getValue());
			}
				} );

		Map<K, V> result = new LinkedHashMap<>();
		int index = 1;
		for (Map.Entry<K, V> entry: list)
		{
			if (index < maxCounter) {
				result.put(entry.getKey(), entry.getValue() );
				index ++;
			}

		}
		return result;
	}
}