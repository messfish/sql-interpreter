package logicalqueryplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import cs4321.operator.PhysicalPlanBuilder;
import cs4321.support.Catalog;

/**
 * this class is the logical version of the select operators.
 * it calls the physical select operator.
 * @author messfish
 *
 */
public class SelectOperators extends Operators {

	private Catalog catalog = Catalog.getInstance();
	private double[][] list; 
	// this data structure stores the upper bound and lower bound for each attribute.
	private Expression result;
	private List<String> dummy;
	private String table;
	private List<String> list1;
	private Map<String, String> hash;
	private Map<String, double[]> V; // this map stores the range value to the respective column.
	private Map<String, Double> Vvalue; // this map stores the V value to the respective column. 
	
	/**
	 * constructor: this helps build the PlainSelect from the list and the 
	 * union find data structure. For each attribute, set a lower and higher
	 * bound for those values.
	 * @param table the table name of the logical selection operator.
	 * @param list the list that shall be used as the logical selection part.
	 * @param hash the hash map that connects the table with the catalog.
	 * @param uf the union find data structure.
	 */
	public SelectOperators(String table, List<String> list, 
			Map<String, String> hash, UnionFind uf) {
		this.table = table;
		this.hash = hash;
		this.V = catalog.getStat();
		String str = hash.get(table);
		List<String> column = catalog.getList(str);
		this.list = new double[column.size()][2];
		for(int i=0;i<column.size();i++){
			this.list[i][0] = Double.NEGATIVE_INFINITY;
			this.list[i][1] = Double.POSITIVE_INFINITY;
		}
		list1 = list;
		dummy = new ArrayList<>(column.size());
		for(int i=0;i<column.size();i++)
			dummy.add(table+"."+column.get(i));
		for(Map.Entry<String, Mule> entry:uf.root.entrySet()){
			Set<String> set = entry.getValue().list;
			for(int i=0;i<dummy.size();i++){
				String s = dummy.get(i);
				if(set.contains(s)){
					if(entry.getValue().lb!=null)
						this.list[i][0] = entry.getValue().lb - 1;
					if(entry.getValue().up!=null)
						this.list[i][1] = entry.getValue().up + 1;
				}
			}
		}
		for(String s: list){
			build(s);
		}
		Vvalue = new HashMap<>();
		buildValue();
	}
	
	/**
	 * this is the visitor's method for traversing the plan builder as a tree.
	 * @param visitor the plan builder that will be used as a tree.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void accept(PhysicalPlanBuilder visitor, String s, StringBuilder str) {
		visitor.visit(this,s,str);
	}
	
	/**
	 * test whether the string is a long integer.
	 * @param s the tested string.
	 * @return whether s is a long integer or not.
	 */
	private boolean isNumber(String s) {
		try{
			Long.parseLong(s);
		}catch(Exception e){
			return false;
		}
		return true;
	}
	
	/**
	 * this method tries to choose the column that is not appeared in the union find
	 * set and assign their values to the 2-D list array. If it does not satisfy the 
	 * needs, include that in the plainselect language.
	 * @param s
	 */
	private void build(String s){
		String[] dummy = s.split("\\s+");
		String[] get1 = dummy[0].split("\\.");
		String[] get2 = dummy[2].split("\\.");
		Expression left = null, right = null, result = null;
		if(isNumber(dummy[2])) right = new LongValue(Long.parseLong(dummy[2]));
		else right = new Column(new Table(null, get2[0]),get2[1]);
		if(isNumber(dummy[0])) left = new LongValue(Long.parseLong(dummy[0]));
		else left = new Column(new Table(null, get1[0]),get1[1]);
		if(dummy[1].equals("=")) result = new EqualsTo(left,right);
	    else if(dummy[1].equals(">=")) result = new GreaterThanEquals(left,right);
	    else if(dummy[1].equals(">")) result = new GreaterThan(left,right);
	    else if(dummy[1].equals("<=")) result = new MinorThanEquals(left,right);
	    else if(dummy[1].equals("<")) result = new MinorThan(left,right);
	    else if(dummy[1].equals("<>")) result = new NotEqualsTo(left,right);
		if(this.result==null) this.result = result;
		else{
			Expression answer = new AndExpression(this.result,result);
			this.result = answer;
		}
	}

	/**
	 * this method build the V value to their respective column name.
	 * Store the result in the V value hash map.
	 */
	private void buildValue(){
		String origin = hash.get(table);
		List<String> list = Catalog.getInstance().getList(origin);
		double[] initial = V.get(origin);
		double value = initial[1] - initial[0];
		for(int i=0;i<list.size();i++){
			String temp = origin + "." + list.get(i);
			double[] range = V.get(temp);
			double difference = range[1] - range[0] + 1.0;
			double[] list1 = new double[]{this.list[i][0],this.list[i][1]};
			if(list1[0]==Double.NEGATIVE_INFINITY)
				list1[0] = range[0] - 1;
			if(list1[1]==Double.POSITIVE_INFINITY)
				list1[1] = range[1] + 1;
			double V = list1[1] - list1[0] - 1;
			Vvalue.put(table+"."+list.get(i), V);
			value = value * V / difference;
		}
		value = (int)value;
		if(value==0.0) value = 1.0;
		Vvalue.put(table, value);
		for(Map.Entry<String, Double> entry : Vvalue.entrySet())
			Vvalue.put(entry.getKey(), Math.min(entry.getValue(),value));
	}
	
	/**
	 * return the list of lower and upper bounds for the use of physical plan builder.
	 * @return the list of lower and upper bounds.
	 */
	public double[][] getPara(){
		return list;
	}
	
	/**
	 * return the expression that will be used for the physical plan builder.
	 * @return the basic expression.
	 */
	public Expression getResult(){
		return result;
	}
	
	/**
	 * return the table that is the name of this relation
	 * @return the name of this relation.
	 */
	public String getTable(){
		return table;
	}
	
	/**
	 * return the map contains the columns and their V-value.
	 * @return the map contains the columns and their V-value.
	 */
	public Map<String,Double> getV() {
		return Vvalue;
	}
	
	/**
	 * this is the print method that is used for generating the logical query plan.
	 * @param String the inital String that will be used for printing.
	 * @param str the string that will be used for writing the file. 
	 */
	@Override
	public void print(String s, StringBuilder str) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(int i=0;i<list1.size();i++)
			sb.append(list1.get(i)).append(" AND ");
		for(int i=0;i<list.length;i++){
			if(list[i][0]!=Double.NEGATIVE_INFINITY&&list[i][1]!=Double.POSITIVE_INFINITY){
				if(list[i][1]-list[i][0]==2){
					sb.append(dummy.get(i)).append(" = ").append((int)list[i][0]+1);
					sb.append(" AND ");
				}else{
					sb.append(dummy.get(i)).append(" >= ").append((int)list[i][0]+1);
					sb.append(" AND ");
					sb.append(dummy.get(i)).append(" <= ").append((int)list[i][1]-1);
					sb.append(" AND ");
				}	
			}else if(list[i][0]!=Double.NEGATIVE_INFINITY){
				sb.append(dummy.get(i)).append(" >= ").append((int)list[i][0]+1);
				sb.append(" AND ");
			}else if(list[i][1]!=Double.POSITIVE_INFINITY){
				sb.append(dummy.get(i)).append(" <= ").append((int)list[i][1]-1);
				sb.append(" AND ");
			}
		}
		if(sb.length()==1){
			sb.append(hash.get(table)).append("]");
			str.append(s+"Leaf"+sb.toString()).append("\n");
		}else{
			sb.deleteCharAt(sb.length()-1);
			sb.deleteCharAt(sb.length()-1);
			sb.deleteCharAt(sb.length()-1);
			sb.deleteCharAt(sb.length()-1);
			sb.deleteCharAt(sb.length()-1);
			sb.append("]");
			str.append(s+"Select"+sb.toString()).append("\n");
			sb = new StringBuilder();
			sb.append("[").append(hash.get(table)).append("]");
			str.append(s+"-"+"Leaf"+sb.toString()).append("\n");
		}
	}
	
}
