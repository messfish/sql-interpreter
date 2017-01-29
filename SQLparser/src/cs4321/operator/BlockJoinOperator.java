package cs4321.operator;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import cs4321.project2.EvaluationCustom;
import cs4321.project2.Interpreter;
import cs4321.support.Catalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This class handles the block join operator,
 * it extends the join operator to generate the same constructor.
 * use the evaluation to do the block join.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class BlockJoinOperator extends JoinOperator {

	private ScanOperator ex; // the scan operator used for output.
	
	/**
	 * Constructor: extends the constructor from the join operator 
	 * and pass the number of pages to the evaluation test.
	 * @param array an array of table names
	 * @param ps the plain select
	 * @param hash the connection of table name and scheme name
	 * @param pages the number of pages used for block nested loop join.
	 * @param map2 the map connects the table and selection operators.
	 * @param map3 the map connects the table and their tuples.
	 * @param express the expression used for the join operator.
	 * @param isAllEqual this array tells whether this join is suitable for SMJ or not.
	 * @param index this number tells the number of buffer pages for BNLJ.
	 * @param previous this array tells the length of the tuple in the previous join.
	 */
	@SuppressWarnings("unchecked")
	public BlockJoinOperator(String[] array, PlainSelect ps,
			Map<String, String> hash, int pages, int pages1, Map<String, SelectOperator> map2,
			Map<String, Tuple> map3, Expression express, boolean[] isAllEqual, int index, int[] previous) {
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
		EvaluationCustom eva = new EvaluationCustom(map2,array,map3,hash,pages,index,previous);
		express.accept(eva);
		File file = new File(Interpreter.getTemp() + "dummy " + (index));
		ex = new ScanOperator(file);
	}

	/**
	 * return the next Tuple.
	 * @return the tuple on the next pointer.
	 */
	@Override
	public Tuple getNextTuple(){
		return ex.getNextTuple();
	}
	
	/**
	 * reset the point back to the start of the table.
	 */
	@Override
	public void reset() {
		ex.reset();
	}
	
}
