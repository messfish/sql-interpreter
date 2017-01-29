package cs4321.operator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cs4321.support.Catalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * this class handles the join operator of the SQL language.
 * We reconstruct the join tree to optimize the speed of join operator.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class JoinOperator extends Operator {

	protected Map<String,SelectOperator> map2; // maps the table with their selection counterpart.
	protected Map<String,Tuple> map3; // map the table with their respective tuples.
	protected Map<String,String> hash; // map the aliases with their respective tables.
	protected Expression express; 
	protected String[] array;
    protected PlainSelect ps;
    protected List<SelectItem> list;
    protected Map<String, Integer> location; // maps the string with the column number.
    protected JoinOperator[] joinList;
    private SelectOperator seo; // this is used in the case when there is only one table.

    /**
     * this empty constructor is used to avoid compiler error. It provides a constructor
     * that could be used by the BNLJ operator and SMJ operator.
     */
    public JoinOperator() {}
    
    /**
     * constructor: build a join constructor, in this constructor, call methods
     * to rebuild the expression tree where all the selections come first and 
     * all joins come next.  
     * @param array an array of table names.
     * @param ps the plain select of a query.
     * @param hash the map between aliases and original table name.
     * @param map2 the map connects the table and selection operators.
	 * @param map3 the map connects the table and their tuples.
	 * @param express the expression used for the join operator.
	 * @param isAllEqual this array holds whether this join is suitable for SMJ or not.
	 * @param index2 the number of buffer pages the BNLJ could use.
	 * @param index4 the number of buffer pages the External Sort could use.
     */
	@SuppressWarnings("unchecked")
	public JoinOperator(String[] array, PlainSelect ps, Map<String,String> hash,
			Map<String, SelectOperator> map2, Map<String, Tuple> map3, Expression express,
			boolean[] isAllEqual, int index2, int index4) {
		seo = map2.get(array[0]);
		this.ps = ps;
		this.hash = hash;
		this.array = array;
		this.map2 = map2;
		this.map3 = map3;
		this.express = express;
		location = new HashMap<>();
		list = ps.getSelectItems();
		int index = 0;
		joinList = new JoinOperator[isAllEqual.length];	
		for(int i=0;i<array.length;i++){
			Map<String,Integer> dum = Catalog.getInstance().getSchema(hash.get(array[i]));
			for(Map.Entry<String, Integer> entry: dum.entrySet()){
				String[] s1 = entry.getKey().split("\\.");
				location.put(array[i]+"."+s1[1], index+entry.getValue());
			}
			index += dum.size();
		}
		int[] previous = new int[array.length];
		for(int i=1;i<previous.length;i++){
			String s = hash.get(array[i-1]);
			List<String> list = Catalog.getInstance().getList(s);
			previous[i] += previous[i-1] + list.size();
		}
		Set<String> visited = new HashSet<>();
		for(int i=0;i<isAllEqual.length;i++){
			visited.add(array[i]);
			if(!isAllEqual[i])
				joinList[i] = new BlockJoinOperator(array,ps,hash,index2,index4,map2,map3,express,isAllEqual,i+1,previous);
			else
				joinList[i] = new SortMergeJoinOperator(array,ps,hash,index2,index4,map2,map3,express,isAllEqual,i+1,visited);
		}
	}
	
	/**
	 * get the next tuple of the operator.
	 * @return the next tuple.
	 */
	@Override
	public Tuple getNextTuple() {
		// TODO Auto-generated method stub
		if(array.length==1) return seo.getNextTuple();
		else{
			Tuple tuple = joinList[joinList.length-1].getNextTuple();
			return tuple;
		}
	}

	/**
	 * reset the operator
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		for(int i=0;i<array.length;i++)
			map2.get(array[i]).reset();
		map3 = new HashMap<>();
	}
	
}
