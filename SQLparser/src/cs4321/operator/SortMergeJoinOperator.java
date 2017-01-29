package cs4321.operator;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import cs4321.project2.EvaluationMklll;
import cs4321.project2.Interpreter;
import cs4321.support.Catalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This class handles the sort join operator.
 * It extends from Join Operator and use the constructor
 * from that class.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class SortMergeJoinOperator extends JoinOperator {

	private ScanOperator ex; // the external sort operator.
	
	/**
	 * Constructor: extends from the join operator and assign the number of pages.
	 * put all the results in a temporary file and 
	 * create a scan operator to do it.
	 * @param array an array of strings.
	 * @param ps the plain select language.
	 * @param hash the connection between aliases.
	 * @param pages the number of pages that shall be used.
	 * @param map2 the map connects the table and selection operators.
	 * @param map3 the map connects the table and their tuples.
	 * @param express the expression used for the join operator.
	 * @param isAllEqual this array tells whether this join is suitable for SMJ or not.
	 * @param index this number tells the number of buffer pages for BNLJ.
	 * @param visited this hash table tells whether the column to be sorted is already in the child join list.
	 */
	@SuppressWarnings("unchecked")
	public SortMergeJoinOperator(String[] array, PlainSelect ps,
			Map<String, String> hash, int pages, int pages1, Map<String, SelectOperator> map2,
			Map<String, Tuple> map3, Expression express, boolean[] isAllEqual, int index, Set<String> visited) {
		this.ps = ps;
		this.hash = hash;
		this.array = array;
		this.map2 = map2;
		this.map3 = map3;
		this.express = express;
		location = new HashMap<>();
		list = ps.getSelectItems();
		int index1 = 0;
		joinList = new JoinOperator[isAllEqual.length];	
		for(int i=0;i<array.length;i++){
			Map<String,Integer> dum = Catalog.getInstance().getSchema(hash.get(array[i]));
			for(Map.Entry<String, Integer> entry: dum.entrySet()){
				String[] s1 = entry.getKey().split("\\.");
				location.put(array[i]+"."+s1[1], index1+entry.getValue());
			}
			index1 += dum.size();
		}
		EvaluationMklll eva2 = new EvaluationMklll(map2,array,map3,hash,location,pages,index,visited);
		express.accept(eva2);
		File file = new File(Interpreter.getTemp() + "dummy " + (index));
		ex = new ScanOperator(file);
	}

	/**
	 * get the next tuple from this operator.
	 * @return the tuple that shall be returned.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		return ex.getNextTuple();
	}
	
	/**
	 * reset the point back to the start of the table.
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		ex.reset();
	}
	
}
