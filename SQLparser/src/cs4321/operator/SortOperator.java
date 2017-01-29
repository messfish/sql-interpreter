package cs4321.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


/**
 * class that handles the order by query.
 * use collections.sort() to sort the query.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class SortOperator extends Operator implements MoveBack{

	private List<Tuple> list; // list that handles the tuples.
	private final String[] string; // the list that handles the array. 
	private final Map<String,Integer> man; 
	// map contains the connection of column name to their respective index.
	private int index; // the index of the list.
	private int anchor; // the index that serves for the moveback method.
	
	/**
	 * constructor: get all the tuples in the list and sort them.
	 * @param operator the operator needs to be sorted.
	 * @param ps the query language.
	 * @param map the map contains the connection of column name to their respective index.
	 */
	public SortOperator(Operator operator, String[] array, Map<String, Integer> map) {
		list = new ArrayList<Tuple>();
	    string = array;
		man = map;
		Tuple tuple = null;
		while((tuple=operator.getNextTuple())!=null)
			list.add(tuple);
		Collections.sort(list,new Comparator<Tuple>(){
			@Override
			public int compare(Tuple tup1, Tuple tup2) {
				// TODO Auto-generated method stub
				// sort tuples from the order by language first.
				for(int i=0;i<string.length;i++){
					String str = string[i].toString();
					int index = man.get(str);
					if(tup1.getData(index)>tup2.getData(index)) return 1;
					else if(tup1.getData(index)<tup2.getData(index)) return -1;
				}
				// sort tuples by the order of the tuples.
				for(int i=0;i<man.size()&&i<tup1.length();i++){
					if(tup1.getData(i)>tup2.getData(i)) return 1;
					else if(tup1.getData(i)<tup2.getData(i)) return -1;
				}
				return 0;
			}	
		});
	}

	/**
	 * set the point to the index it points for future use.
	 */
	@Override
	public void anchor(){
		anchor = index - 1;
	}
	
	/**
	 * move the index back to the specific point.
	 */
	@Override
	public void moveback(){
		index = anchor;
	}
	
	/**
	 * get the next tuple of the operator.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		Tuple tuple = null;
		if(index<list.size()) tuple = list.get(index);
		index++;
		return tuple;
	}

	/**
	 * reset the operator.
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		index = 0;
	}

}
