package cs4321.operator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cs4321.project2.BTreeBuilder;
import cs4321.project2.Interpreter;
import cs4321.support.Catalog;
import logicalqueryplan.DP;
import logicalqueryplan.DuplicateEliminationOperators;
import logicalqueryplan.JoinOperators;
import logicalqueryplan.LogicalQueryVisitor;
import logicalqueryplan.Mule;
import logicalqueryplan.ProjectOperators;
import logicalqueryplan.SelectOperators;
import logicalqueryplan.SortOperators;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * this is the physical plan builder. build a tree to 
 * get the result by calling the tree recursively.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class PhysicalPlanBuilder implements LogicalQueryVisitor {
	
	private String[] array; // the array of tables.
	private PlainSelect ps; // the selection query of the physical plan.
	private Map<String,String> hash; // the conversion between aliases.
	private Operator op; // the operator that will be used.
	private int index2 = 100; // index to decide the number of pages for joins shall be used.
	private int index4 = 100; // index to decide the number of pages for sort shall be used.
	protected Map<String,SelectOperator> map2; // maps the table with their selection counterpart.
	protected Map<String,Tuple> map3; // map the table with their respective tuples. 
    private Map<String, Integer> map4; // this map connects tables with their indexes.
    private Catalog catalog = Catalog.getInstance();
    protected SelectOperator[] select;
    private Map<SelectOperator, Integer> sei; 
    // this map connects the select operator with their specific index.
    private Map<SelectOperator, double[]> sed;
    private Map<String, Integer> rearrange; 
    private int[] previous;
    private String[] collect;
	
	/**
	 * Constructor: get all those parameters into fields for the sake of
	 * creating physical operators.
	 * @param array the array of tables.
	 * @param ps the plain select language.
	 * @param hash the connection between aliases.
	 * @param previous the array contains the number of previous columns.
	 */
	public PhysicalPlanBuilder(String[] array, PlainSelect ps, Map<String,String> hash, int[] previous){
	    this.array = array;
	    select = new SelectOperator[array.length];
	    this.previous = previous;
	    map4 = new HashMap<>();
	    for(int i=0;i<array.length;i++)
	    	map4.put(array[i], i);
	    map2 = new HashMap<>();
	    map3 = new HashMap<>();
	    this.ps = ps;
	    this.hash = hash;
	    sei = new HashMap<>();
	    sed = new HashMap<>();
	    rearrange = new HashMap<>();
	}

	/**
	 * visitor method for project operator.
	 * do a post traversal of the tree and create a physical operator.
	 * @param operator the operator needs to be visited.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void visit(ProjectOperators operator, String s, StringBuilder str){
		StringBuilder sb = new StringBuilder();
		if(!ps.getSelectItems().get(0).toString().equals("*")){
			sb.append(ps.getSelectItems().toString());
			str.append(s+"Project"+sb.toString()).append("\n");
			operator.getChild().accept(this,s+"-",str);
		}else operator.getChild().accept(this,s,str);
		op = new ProjectOperator((JoinOperator)op,rearrange,previous,collect);
	}
	
	/**
	 * visitor method for join operator.
	 * do a post traversal of the tree and create a physical operator.
	 * @param operator the operator needs to be visited.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void visit(JoinOperators operator, String s, StringBuilder str){
		for(int i=0;i<array.length;i++)
			operator.getChild(i).accept(this,s+"-",str);
		collect = new String[array.length];
		for(int i=0;i<collect.length;i++)
			collect[i] = array[i];
		if(array.length>2){
			DP dp = new DP(operator,array,hash);
			List<String> order = dp.returnOrder();
			for(int i=0;i<array.length;i++)
				array[i] = order.get(i);
		}
		String[] collect1 = operator.getCollection();
		List<String> list = new ArrayList<>();
		for(int i=0;i<collect1.length;i++)
			list.add(collect1[i]);
		extend(operator,list);
		String[] collection = new String[list.size()];
		for(int i=0;i<collection.length;i++)
			collection[i] = list.get(i);
		boolean[] marked = new boolean[collection.length];
		Set<String> visited = new HashSet<>();
		boolean[] isAllEqual = new boolean[array.length-1];
		for(int i=0;i<isAllEqual.length;i++)
			isAllEqual[i] = true;
		Expression result = buildJoin(visited,marked,collection,isAllEqual);
		for(int i=0;i<collect.length;i++)
			map2.put(collect[i],select[i]);
		print(s,result,isAllEqual,str);
		for(int i=0;i<collect.length;i++){
			rearrange.put(collect[i], i);
		}
		op = new JoinOperator(array,ps,hash,map2,map3,result,isAllEqual,index2,index4);
	}
	
	/**
	 * this method generates the physical selection operator.
	 * @param operator the logical selection operator.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void visit(SelectOperators operator, String s, StringBuilder str) {
		Expression express = operator.getResult();
		double[][] list = operator.getPara();
		String table = operator.getTable();
		File file = new File(catalog.getFileLocation(hash.get(table)));
		double[] result = new double[]{Double.NEGATIVE_INFINITY,Double.POSITIVE_INFINITY};
		double[][] store = new double[result.length][2];
		int i = map4.get(table);
		int index = build(list,hash.get(table),store);
		express = buildExpress(express,list,table,index);
		if(index!=-1){
			String column = catalog.getList(hash.get(table)).get(index);
			select[i] = new SelectOperator(file,table,express,hash,list[index],column);
		}else select[i] = new SelectOperator(file,table,express,hash,result,"");
		sei.put(select[i], index);
		if(index!=-1) sed.put(select[i], store[index]);
	}
	
	/**
	 * visitor method for duplicate elimination operator.
	 * do a post traversal of the tree and create a physical operator.
	 * @param operator the operator needs to be visited.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void visit(DuplicateEliminationOperators operator, String s, StringBuilder str) {
		str.append(s+"DupElim").append("\n");
		operator.getChild().accept(this,s+"-",str);
		op = new DuplicateEliminationOperator(op);
	}
	
	/**
	 * visitor method for sort operator.
	 * do a post traversal of the tree and create a physical operator.
	 * @param operator the operator needs to be visited.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void visit(SortOperators operator, String s, StringBuilder str) {
		StringBuilder sb = new StringBuilder();
		if(ps.getOrderByElements()!=null){
			sb.append(ps.getOrderByElements().toString());
			str.append(s+"ExternalSort"+sb.toString()).append("\n");
			operator.getChild().accept(this,s+"-",str);
		}else{
			str.append(s+"ExternalSort"+"[]").append("\n");
			operator.getChild().accept(this,s+"-",str);
		}
		ProjectOperator po = (ProjectOperator)op;
		List<String> list = new ArrayList<>();
		String[] array = null;
		if(ps.getOrderByElements()!=null){
			for(int i=0;i<ps.getOrderByElements().size();i++)
				list.add(ps.getOrderByElements().get(i).toString());
			array = new String[list.size()];
			for(int i=0;i<array.length;i++)
				array[i] = list.get(i);
		}else array = new String[0];
		op = new EXSortOperator(po,array,po.getMap(),index4,1);
	}
	
	/**
	 * spit all the tuples at once, put the results in a file.
	 * @param s the file location in a string form.
	 * @param index the index to identify the query.
	 */
	public void dump(String s, String index){
		op.dump(s, index);
	}
	
	/**
	 * this method is the method that will point out which column will be used for the
	 * B+ tree. If there are no ideal choice, simply use the ordinary selection operator.
	 * @param list the list shows the high value and low value of each column.
	 * @param table the table that is the name of the relation.
	 * @param store this data structure stores the temporary range of values for each attribute.
	 */
	private int build(double[][] list, String table, double[][] store) {
		File file = new File(Interpreter.getInput() + "/db/stats.txt");
		int size = catalog.getList(table).size(), answer = -1;
		List<String> array = catalog.getList(table);
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(file));
			String s = null;
			while((s=buffer.readLine())!=null){
				String[] dummy = s.split("\\s+");
				if(dummy[0].equals(table)){
					int nums = Integer.parseInt(dummy[1]);
					int pages = nums/(1022/size);
					if(nums%(1022/size)!=0) pages++;
					int min = pages;
					for(int i=2;i<dummy.length;i++){
					    if(catalog.getCluster(table+"."+array.get(i-2))==-1)
					    	continue;
						String[] temp = dummy[i].split(",");
						int low = Integer.parseInt(temp[1]);
						store[i-2][0] = low;
						int high = Integer.parseInt(temp[2]);
						store[i-2][1] = high;
						double numbers = high - low + 1;
						double difference = 0.0;
						if(list[i-2][0]!=Double.NEGATIVE_INFINITY&&list[i-2][1]!=Double.POSITIVE_INFINITY)
							difference = list[i-2][1] - list[i-2][0] - 1;
						else if(list[i-2][1]!=Double.POSITIVE_INFINITY)
							difference = list[i-2][1] - low;
						else if(list[i-2][0]!=Double.NEGATIVE_INFINITY)
							difference = high - list[i-2][0];
						else continue;
						if(catalog.getCluster(table+"."+array.get(i-2))==1){
							double use = pages * difference / numbers + 3.0;
							int uses = (int)use;
							if(uses<use) uses++;
							if(uses<min){
								min = uses;
								answer = i - 2;
							}
						}else{
							File fill = new File(Interpreter.getInput()+"/db/indexes/"+table+"."+array.get(i-2));
							int leaves = BTreeBuilder.getNumberOfLeaves(fill);
							double para1 = leaves * difference / numbers;
							int para2 = (int)para1;
							if(para2<para1) para2++;
							double para3 = nums * difference / numbers;
							int para4 = (int)para3;
							if(para4<para3) para4++;
							int end = 3 + para2 + para4;
							if(end<min){
								min = end;
								answer = i - 2;
							}
						}
					}
					break;
				}
			}
			buffer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return answer;
	}
	
	/**
	 * this is the method that build the expression tree with those indexes that will not be
	 * used in the selection part.
	 * @param express the expression that shall be built on.
	 * @param list the list of indexes that shall be used in the future.
	 * @param table the table that is the name of the relation
	 * @param index the index that will be used for the B+ Tree.
	 * @return the modified expression
	 */
	private Expression buildExpress(Expression express, double[][] list, String table, int index) {
		List<String> column = catalog.getList(hash.get(table));
		for(int i=0;i<list.length;i++){
			if(i!=index){
				Expression left = null, right = null, result = null;
				Expression left1 = null, right1 = null, result1 = null;
				if(list[i][0]!=Double.NEGATIVE_INFINITY&&list[i][1]!=Double.POSITIVE_INFINITY){
					if(list[i][1]-list[i][0]==2.0){
						left = new Column(new Table(null, table), column.get(i));
						right = new LongValue((long)(list[i][0]+1));
						result = new EqualsTo(left,right);
					}else{
						left = new Column(new Table(null, table), column.get(i));
						right = new LongValue((long)list[i][0]);
						result = new GreaterThan(left,right);
					    left1 = new Column(new Table(null, table), column.get(i));
						right1 = new LongValue((long)list[i][1]);
						result1 = new MinorThan(left1,right1);
					}
				}else if(list[i][0]!=Double.NEGATIVE_INFINITY){
					left = new Column(new Table(null, table), column.get(i));
					right = new LongValue((long)list[i][0]);
					result = new GreaterThan(left,right);
				}else if(list[i][1]!=Double.POSITIVE_INFINITY){
					left = new Column(new Table(null, table), column.get(i));
					right = new LongValue((long)list[i][1]);
					result = new MinorThan(left,right);
				}
				if(result!=null){
					if(express==null) express = result;
					else{
						Expression answer = new AndExpression(express,result);
						express = answer;
					}
				}
				if(result1!=null){
					if(express==null) express = result1;
					else{
						Expression answer = new AndExpression(express,result1);
						express = answer;
					}
				}
			}
		}
		return express;
	}
	
	/**
	 * this method puts the join operators in the join list and store the result
	 * in the arraylist.
	 * @param operator the logical join operators that will be performed.
	 * @param list the list of string that will be used to store join conditions.
	 */
	private void extend(JoinOperators operator, List<String> list) {
		Map<String, String> root = operator.getRoot();
		Map<String, Mule> map = operator.getUnion();
		for(int i=0;i<array.length;i++){
			List<String> column = catalog.getList(hash.get(array[i]));
			for(String s: column){
				String str = array[i] + "." + s;
				String dummy = str;
				while(!root.get(dummy).equals(" "))
					dummy = root.get(dummy);
				if(map.containsKey(dummy)){
					Set<String> set = map.get(dummy).getList();
					for(String st : set){
						String[] temp = st.split("\\.");
						if(!array[i].equals(temp[0])){
							String combine = str + " = " + st;
							list.add(combine);
						}
					}
					set.remove(str);
				}
			}
		}
	}
	
	/**
	 * construct a left inclined tree for all the joins.
	 * @param set check whether the tables are included
	 * @param marked check whether the element in the list is already in the tree.
	 * @param collection an array of join collections.
	 * @param isAllEqual mark whether this join could be treated by SMJ.
	 * @return the left inclined join tree.
	 */
	private Expression buildJoin(Set<String> set, boolean[] marked,
			                     String[] collection, boolean[] isAllEqual) {
		Expression left = null, right = null, answer = new Parenthesis(null);
		set.add(array[0]);
		for(int i=1;i<array.length;i++){
			set.add(array[i]);
			Expression result = null;
			for(int j=0;j<collection.length;j++){
				if(!marked[j]){
					String[] temp = collection[j].split("\\s+");
				    String[] dummy1 = temp[0].split("\\.");
				    String[] dummy2 = temp[2].split("\\.");
				    // check whether the tables are valid to be used.
				    if(set.contains(dummy1[0])&&set.contains(dummy2[0])){
					    left = new Column(new Table(null,dummy1[0]), dummy1[1]);
					    right = new Column(new Table(null,dummy2[0]), dummy2[1]);
					    Expression temp1 = result, temp2 = null;
					    if(temp[1].equals("=")) temp2 = new EqualsTo(left,right);
			            else if(temp[1].equals(">=")) temp2 = new GreaterThanEquals(left,right);
			            else if(temp[1].equals(">")) temp2 = new GreaterThan(left,right);
			            else if(temp[1].equals("<=")) temp2 = new MinorThanEquals(left,right);
			            else if(temp[1].equals("<")) temp2 = new MinorThan(left,right);
			            else if(temp[1].equals("<>")) temp2 = new NotEqualsTo(left,right);
					    // construct a left inclined tree for the expressions.
					    if(result==null) result = temp2;
					    else result = new AndExpression(temp1,temp2);
					    marked[j] = true;
					    if(!temp[1].equals("=")) isAllEqual[i-1] = false;
				    }
				}
			}
			// if the join is simply a cross product, create an artificial node.
			if(result==null) result = new LongValue(0);
			// create a left inclined join tree.
			Expression temp = new AndExpression(answer,result);
			// use a parenthesis node as check point.
			answer = new Parenthesis(temp);
		}
		return answer;
	}
	
	/**
	 * this method is used to print the physical query plan.
	 * @param s the string indicates the level of the tree.
	 * @param result the expression that will be used for printing the join condition.
	 * @param isAllEqual the boolean array that tells whether this join condition
	 * contains all equal statements or not.
	 * @param build the string that will be used for writing the file.
	 */
	private void print(String s, Expression result, boolean[] isAllEqual, StringBuilder build) {
		Map<String, Set<String>> map = new HashMap<>();
		StringBuilder str = new StringBuilder(s);
		for(int i=isAllEqual.length-1;i>=0;i--){
			Parenthesis temp = (Parenthesis)result;
			AndExpression and = (AndExpression)temp.getExpression();
			Expression express = and.getRightExpression();
			result = and.getLeftExpression();
			if(express.toString().equals("0"))
				build.append(str.toString()+"BNLJ"+"[]").append("\n");
			else if(!isAllEqual[i])
				build.append(str.toString()+"BNLJ"+"["+express.toString()+"]").append("\n");
			else{
				String[] dummy = express.toString().split("AND");
				for(int j=0;j<dummy.length;j++){
					String[] dummy1 = dummy[j].trim().split("\\s+");
					String[] dummy2 = dummy1[0].split("\\.");
					String[] dummy3 = dummy1[2].split("\\.");
					if(!map.containsKey(dummy2[0]))
						map.put(dummy2[0], new HashSet<>());
					if(!map.containsKey(dummy3[0]))
						map.put(dummy3[0], new HashSet<>());
					map.get(dummy2[0]).add(dummy1[0]);
					map.get(dummy3[0]).add(dummy1[2]);
				}
				build.append(str.toString()+"SMJ"+"["+express.toString()+"]").append("\n");
			}
			str.append("-");
		}
		for(int i=0;i<array.length;i++){
			StringBuilder sb = new StringBuilder(str.toString());
			if(map.containsKey(array[i])){
				Set<String> store = map.get(array[i]);
				StringBuilder sb1 = new StringBuilder();
				sb1.append("[");
				for(String st : store)
					sb1.append(st).append(", ");
				sb1.deleteCharAt(sb1.length()-1);
				sb1.deleteCharAt(sb1.length()-1);
				sb1.append("]");
				build.append(sb.toString()+"ExternalSort"+sb1.toString()).append("\n");
				sb.append("-");
			}
			SelectOperator so = map2.get(array[i]);
			Expression express = so.getExpress();
			if(express!=null){
				build.append(sb.toString()+"Select"+"["+express.toString()+"]").append("\n");
				sb.append("-");
			}
			int index = sei.get(so);
			String original = hash.get(array[i]);
			if(index!=-1){
				double[] cache = sed.get(so);
				double[] values = so.getIndexes();
				double low = Math.max(cache[0],values[0]+1);
				double high = Math.min(cache[1], values[1]-1);
				String column = catalog.getList(original).get(index);
				StringBuilder sb2 = new StringBuilder();
				sb2.append("[").append(original).append(", ").append(column).append(", ");
				sb2.append((int)low).append(", ").append((int)high).append("]");
				build.append(sb.toString()+"IndexScan"+sb2.toString()).append("\n");
			}else build.append(sb.toString()+"TableScan"+"["+original+"]").append("\n");
			if(i>0) str.deleteCharAt(str.length()-1);
		}
	}
	
}
