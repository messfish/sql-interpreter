package logicalqueryplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import cs4321.support.Catalog;


/**
 * for this class, I choose the order of the join by using the dynamic programming 
 * as specified in the instructions. Generating all of the subsets possible and 
 * calculate their expected outcomes and store them in a specific data structure.
 * Save the order that has the lowest cost and return it back to the Physical 
 * Plan Builder for the future work.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class DP {

	private String[] array; // the array stores the list of all tables.
	private List<String> output; // this array stores the final join order of this query.
	private Map<String, String> union; // this is the union find data structure.
	private Map<String, Mule> root; // this connects the root with the union data structure.
	private double cost = Double.POSITIVE_INFINITY;
	private Map<String, String> hash;
	
	/**
	 * constructor: this constructor initialize all the data structures we need to perform
	 * the dynamic programming. generate the final join order and put that result in a
	 * String array in a field that will be returned.
	 * @param operator the join operator that will be used.
	 * @param array the array that contains the name of all tables.
	 * @param hash the map that connects the table name to their original name.
	 */
	public DP(JoinOperators operator, String[] array, Map<String, String> hash) {
		this.array = array;
		this.hash = hash;
		union = operator.getRoot();
		root = operator.getUnion();
		Queue<List<String>> queue1 = new LinkedList<>();
		Queue<Map<String,Double>> queue2 = new LinkedList<>();
		Queue<double[]> queue3 = new LinkedList<>();
		Map<String, Double> all = new HashMap<>();
		for(int i=0;i<array.length;i++){
			Map<String, Double> doom = operator.getChild(i).getV();
			for(Map.Entry<String, Double> entry : doom.entrySet()){
				all.put(entry.getKey(), entry.getValue());
			}
		}
		initialize(queue1,queue2,queue3,all);
		runDP(queue1,queue2,array,hash,queue3);
		for(int i=0;i<array.length;i++){
			if(output.indexOf(array[i])==-1){
				output.add(array[i]);
				break;
			}
		}
	}
	
	/**
	 * this method stores the initialization of the queues: the first queue stores a single
	 * distinctive table. The second table stores the V value for all of those columns. The 
	 * last table stores the intermediate I/O for the current join order.
	 * @param queue1 the queue stores the join order.
	 * @param queue2 the queue stores the V value to all of those columns.
	 * @param queue3 the queue stores the intermediate I/O for the current join order.
	 * @param all the map contains the V value and their respective column name.
	 */
	private void initialize(Queue<List<String>> queue1, Queue<Map<String,Double>> queue2,
			                Queue<double[]> queue3, Map<String, Double> all) {
		for(int i=0;i<array.length;i++){
			List<String> list = new ArrayList<>();
			list.add(array[i]);
			queue1.offer(list);
			Map<String, Double> state = copy(all);
			queue2.offer(state);
			double value = all.get(array[i]);
			queue3.offer(new double[]{0.0,value});
		}
	}

	/**
	 * return the arraylist of join orders.
	 * @return an arraylist of join orders.
	 */
	public List<String> returnOrder() {
		return output;
	}
	
	/**
	 * this is the main part of the dynamic programming. Use BFS to generate all the join
	 * order subsets with the same length and calculate their intermediate I/O cost. Store
	 * the result in the queue3 and put the changes of V values into queue2. 
	 * When the length of the join order is 1 lower than the 
	 * original join array, compare the result with the intermediate I/O. If it is smaller,
	 * put the join result into the destination.
	 * @param queue1 the queue stores the join order.
	 * @param queue2 the queue stores the V value to all of those columns.
	 * @param array the list of array that contains the table names.
	 * @param hash the map that connects the table name with their original name.
	 * @param queue3 the queue stores the intermediate I/O cost.
	 */
	private void runDP(Queue<List<String>> queue1, Queue<Map<String,Double>> queue2,
			                String[] array, Map<String, String> hash, Queue<double[]> queue3) {
		int level = 1;
		while(level<array.length-1){
			int size = queue1.size();
			for(int i=0;i<size;i++){
				List<String> list = queue1.poll();
				Map<String, Double> cost = queue2.poll();
				double[] io = queue3.poll();
				Set<String> set1 = new HashSet<>();
				Set<String> set2 = new HashSet<>();
				for(int j=0;j<list.size();j++){
					set1.add(list.get(j));
					set2.add(list.get(j));
				}
				for(int j=0;j<array.length;j++){
					if(!set1.contains(array[j])){
						set2.add(array[j]);
						List<String> list1 = new ArrayList<>();
						for(String str : list)
							list1.add(str);
						list1.add(array[j]);
						Map<String, Double> cost1 = copy(cost);
						double[] io1 = new double[]{io[0],io[1]};
						calculateCost(cost1,io1,set1,set2,array[j]);
						if(level==array.length-2&&this.cost>io1[0]){
							this.cost = io1[0];
							output = new ArrayList<>();
							for(int k=0;k<list1.size();k++){
								output.add(list1.get(k));
							}
						}
						queue1.offer(list1);
						queue2.offer(cost1);
						queue3.offer(io1);
						set2.remove(array[j]);
					}
				}
			}
			level++;
		}
	}
	
	/**
	 * this method perform a copy of the current map. Return that map.
	 * @param all the map that is gonna to be copied.
	 * @return the copy of the map.
	 */
	private Map<String, Double> copy(Map<String, Double> all) {
		Map<String, Double> result = new HashMap<>();
		for(Map.Entry<String, Double> entry : all.entrySet())
			result.put(entry.getKey(), entry.getValue());
		return result;
	}
	
	/**
	 * this method calculate the expected join cost of the join conditions. If all the 
	 * table names are appeared in set2 and not all of them are appeared in set1, we could 
	 * tell this is the join condition that we want to calculate. Do the calculation based 
	 * on the instructions and store the change in cost1 and io1.
	 * @param cost1 this map stores the V value of all attributes.
	 * @param io1 the array stores the intermediate I/O for the join order.
	 * @param set1 the hash set that contains the left part of the join.
	 * @param set2 the hash set that contains the whole tables of the join.
	 * @param outer this string stores the table name that is the outer side of the join.
	 */
	private void calculateCost(Map<String, Double> cost1, double[] io1,
			                   Set<String> set1, Set<String> set2, String outer) {
		double value = cost1.get(outer) * io1[1];
		Map<String, List<String>> equals = new HashMap<>();
		List<String> column = Catalog.getInstance().getList(hash.get(outer));
		for(String s: column){
			String dummy = outer + "." + s;
			String root = dummy;
			while(!union.get(root).equals(" "))
				root = union.get(root);
			if(this.root.containsKey(root)){
				Mule mule = this.root.get(root);
				Set<String> set = mule.list;
				double max = 0;
				for(String str : set){
					String[] string = str.split("\\.");
					if(set1.contains(string[0])){
						max = Math.max(max, cost1.get(str));
						if(!equals.containsKey(dummy))
							equals.put(dummy,new ArrayList<>());
						equals.get(dummy).add(str);
					}
				}
				if(max>0){
					value = value/max;
				}
			}
		}
		value = (int)value;
		if(value==0.0) value = 1.0;
		io1[0] += value;
		io1[1] = value;
		for(Map.Entry<String, List<String>> entry : equals.entrySet()) {
			List<String> list = entry.getValue();
			list.add(entry.getKey());
			double min = Double.POSITIVE_INFINITY;
			for(String s : list){
				double doubt = cost1.get(s);
				min = Math.min(min, doubt);
			}
			for(String s : list)
				cost1.put(s, min);
		}
		for(Map.Entry<String, Double> entry : cost1.entrySet()){
			String[] test = entry.getKey().split("\\.");
			if(test.length>1&&set2.contains(test[0]))
				cost1.put(entry.getKey(), Math.min(entry.getValue(),value));
		}
	}
	
}
