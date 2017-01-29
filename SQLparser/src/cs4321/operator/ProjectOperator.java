package cs4321.operator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cs4321.support.Catalog;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * This is the class that handles the projection part.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class ProjectOperator extends Operator{

	private JoinOperator jo; // the join operator as candidate.
	private boolean isAsterik = true; // check whether we need to modify the tuple.
	private Map<String,String> hash; // map the aliases with their respective tables.
	private Map<String,Integer> location;
	// the location of a column to the tuple index. Used in the sort operator.
    private int[] prev;
	private Map<String, Integer> rearrange;
	
	/**
	 * Constructor: create a project operator based on a join operator.
	 * store some important message in the location for sorting.
	 * @param jo the JoinOperator.
	 * @param rearrange the map that links the new join order with the original one.
	 * @param prev the map that stores how many columns before this table.
	 * @param collect the array that stores the original order.
	 */
	public ProjectOperator(JoinOperator jo, Map<String, Integer> rearrange, int[] prev, String[] collect) {
		this.jo = jo;
		this.hash = jo.hash;
		this.rearrange = rearrange;
		this.prev = prev;
		location = new HashMap<>();
		@SuppressWarnings("unchecked")
		List<SelectItem> list = jo.ps.getSelectItems();
		if(!list.get(0).toString().equals("*")){
			isAsterik = false;
			for(int i=0;i<list.size();i++){
				location.put(list.get(i).toString(), i);
			}
		}
		else{
			int index = 0;
			for(int i=0;i<collect.length;i++){
				Map<String,Integer> dum = Catalog.getInstance().getSchema(hash.get(collect[i]));
				for(Map.Entry<String, Integer> entry: dum.entrySet()){
					String[] s1 = entry.getKey().split("\\.");
					location.put(collect[i]+"."+s1[1], index+entry.getValue());
				}
				index += dum.size();
			}
		}
	}

	/**
	 * get the nextTuple of the result.
	 * @return the tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		Tuple tuple = jo.getNextTuple();
		if(!isAsterik&&tuple!=null){
		    int[] data = new int[jo.list.size()];
		    for(int i=0;i<data.length;i++){
		        String str = jo.list.get(i).toString();
		        data[i] = tuple.getData(jo.location.get(str));
		    }
		    tuple = new Tuple(data);
		}
		if(isAsterik&&tuple!=null){
			int[] dummy = new int[tuple.length()];
			for(int i=0;i<tuple.length();i++)
				dummy[i] = tuple.getData(i);
			int[] data = new int[tuple.length()];
			int pointer = 0;
			for(int i=0;i<jo.array.length;i++){
				int index = rearrange.get(jo.array[i]);
				int size = Catalog.getInstance().getList(hash.get(jo.array[i])).size();
				for(int j=0;j<size;j++){
					data[prev[index]+j] = dummy[pointer];
					pointer++;
				}
				tuple = new Tuple(data);
			}
		}
		return tuple;
	}

	/**
	 * reset the pointer to the starting point.
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		jo.reset();
	}

    /**
     * returns the location map which connect the column with their indexes.
     * @return the location map.
     */
    public Map<String,Integer> getMap(){
    	return location;
    }

}
