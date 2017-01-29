package cs4321.project2;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cs4321.operator.EXSortOperator;
import cs4321.operator.MoveBack;
import cs4321.operator.Operator;
import cs4321.operator.ScanOperator;
import cs4321.operator.SelectOperator;
import cs4321.operator.Tuple;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * this is the class that implements the sort merge join.
 * Sort and merge the two tables by using the algorithm in 
 * the text book. After the sorting is finished, put them in
 * a temporary file which will be used for the next join.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class EvaluationMklll extends EvaluationMkll implements ExpressionVisitor {

	private Operator op;
	private List<String> temp1;
	private List<String> temp2;
	private Set<String> marked;
	private int pages;
	private Map<String, Integer> location;
	private File file;
	private int index;
	
	/**
	 * constructor: extend the constructor from the super method.
	 * @param map the map connects to the string with the select operator.
	 * @param array an array shows the list of table names.
	 * @param map2 the map connects the table name with their tuples.
	 * @param hash the map connects the table name with their schema name.
	 * @param location the map connects the column with the point.
	 * @param pages the number of pages that will be used for external sorting.
	 */
	public EvaluationMklll(Map<String, SelectOperator> map, String[] array,
			Map<String, Tuple> map2, Map<String, String> hash, Map<String, Integer> location,
			int pages, int index, Set<String> visited) {
		super(map, array, map2, hash);
	    marked = visited;
		this.pages = pages;
		this.location = location;
		this.index = index;
	}
	
	/**
	 * merge the two sorted tables by using the algorithm in page 460.
	 * @param ex1 the left operator of the merge
	 * @param ex2 the right operator of the merge
	 * @param dummy1 the sort part for the first operator.
	 * @param dummy2 the sort part for the second operator.
	 * @param location1 the map connects the column with the point.
	 * @param location2 the map connects the column with the point.
	 */
	public void merge(Operator ex1, MoveBack ex2, String[] dummy1, String[] dummy2,
			Map<String, Integer> location1, Map<String, Integer> location2){
		int index = 8, times = 0;
		try{
			FileOutputStream fout = new FileOutputStream(file);
		    FileChannel fc = fout.getChannel();
		    ByteBuffer byt = ByteBuffer.allocate(4096);
		    ByteBuffer buffer = ByteBuffer.allocate(4096);
		    Tuple tuple1 = ex1.getNextTuple(), tuple2 = ex2.getNextTuple();
		    if(tuple1==null||tuple2==null){
		    	fout.close();
		    	return;
		    }
		    while(tuple1!=null&&tuple2!=null){
		    	if(compare(tuple1,tuple2,dummy1,dummy2,location1,location2)<0)
		    		tuple1 = ex1.getNextTuple();
		    	else if(compare(tuple1,tuple2,dummy1,dummy2,location1,location2)>0)
		    		tuple2 = ex2.getNextTuple();
		    	else{
		    		ex2.anchor();
		    		while(tuple2!=null&&compare(tuple1,tuple2,dummy1,dummy2,location1,location2)==0){
		    			if(index+tuple1.length()*4+tuple2.length()*4<=4096){
		    				buffer.putInt(0,tuple1.length()+tuple2.length());
		    				for(int i=0;i<tuple1.length();i++){
		    					buffer.putInt(index,tuple1.getData(i));
		    					index += 4;
		    				}
		    				for(int i=0;i<tuple2.length();i++){
		    					buffer.putInt(index,tuple2.getData(i));
		    					index += 4;
		    				}
		    				times++;
		    			}else{
		    				buffer.putInt(4,times);
		    				byt = buffer;
		    				byt.position(0);
		    				fc.write(byt);
		    				buffer = ByteBuffer.allocate(4096);
		    				index = 8;
		    				times = 0;
		    				buffer.putInt(0,tuple1.length()+tuple2.length());
		    				for(int i=0;i<tuple1.length();i++){
		    					buffer.putInt(index,tuple1.getData(i));
		    					index += 4;
		    				}
		    				for(int i=0;i<tuple2.length();i++){
		    					buffer.putInt(index,tuple2.getData(i));
		    					index += 4;
		    				}
		    				times++;
		    			}
		    			tuple2 = ex2.getNextTuple();
		    		}
		    		if(tuple2==null) ex2.reset();
		    		ex2.moveback();
		    		tuple1 = ex1.getNextTuple();
		    		tuple2 = ex2.getNextTuple();
		    	}
		    }
		    buffer.putInt(4,times);
		    byt = buffer;
			byt.position(0);
			fc.write(byt);
			fout.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * method that compares the two tuples.
	 * @param tuple1 the tuple1 serves as candidate for comparation
	 * @param tuple2 the tuple2 serves as candidate for comparation
	 * @param dummy1 the array of column names for tuple1.
	 * @param dummy2 the array of column names for tuple2.
	 * @param location1 the connection of column names with their indexes for tuple1.
	 * @param location2 the connection of column names with their indexes for tuple2.
	 * @return the integer tells the result: 1 means first one is larger
	 * -1 means the second one is larger
	 * 0 means the two tuples are the same.
	 */
	public int compare(Tuple tuple1, Tuple tuple2, String[] dummy1, String[] dummy2,
			Map<String, Integer> location1, Map<String, Integer> location2){
		for(int i=0;i<dummy1.length;i++){
			int index1 = location1.get(dummy1[i]);
			int index2 = location2.get(dummy2[i]);
			int temp1 = tuple1.getData(index1);
			int temp2 = tuple2.getData(index2);
			if(temp1<temp2) return -1;
			if(temp1>temp2) return 1;
		}
		return 0;
	}
	
	@Override
	public void visit(NullValue node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue node) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * visiter method for the parenthesis node.
	 * recursively left travel the join tree. Do the sort merge join by using 
	 * the paramenters coming from the right tree. When it is finished, put the
	 * result in the temp resource file which will be used for the next join.
	 * @param node the parenthesis node to be visited
	 */
	@Override
	public void visit(Parenthesis node) {
		// TODO Auto-generated method stub
		if(length!=index){
			while(length!=index){
				Expression exp = node.getExpression();
				AndExpression ae = (AndExpression)exp;
				node = (Parenthesis)ae.getLeftExpression();
				length--;
			}
		}
		if(index==1){
			temp1 = new ArrayList<>();
			temp2 = new ArrayList<>();
			Expression exp = node.getExpression();
			AndExpression ae = (AndExpression)exp;
			ae.getRightExpression().accept(this);
			SelectOperator seo1 = map.get(array[0]);
			SelectOperator seo2 = map.get(array[1]);
			String[] dummy1 = new String[temp1.size()];
			String[] dummy2 = new String[temp2.size()];
			for(int i=0;i<temp1.size();i++)
				dummy1[i] = temp1.get(i);
			for(int i=0;i<temp2.size();i++)
				dummy2[i] = temp2.get(i);
		    op = new EXSortOperator(seo1,dummy1,seo1.getLocation(),pages,0);
			MoveBack ex2 = null;
			ex2 = new EXSortOperator(seo2,dummy2,seo2.getLocation(),pages,1);
			file = new File(Interpreter.getTemp() + "dummy " + index);
			merge(op,ex2,dummy1,dummy2,seo1.getLocation(),seo2.getLocation());
			op = new ScanOperator(file);
		}else{
			Expression exp = node.getExpression();
			AndExpression ae = (AndExpression)exp;
			temp1 = new ArrayList<>();
			temp2 = new ArrayList<>();
			ae.getRightExpression().accept(this);
			String[] dummy1 = new String[temp1.size()];
			String[] dummy2 = new String[temp2.size()];
			for(int i=0;i<temp1.size();i++)
				dummy1[i] = temp1.get(i);
			for(int i=0;i<temp2.size();i++)
				dummy2[i] = temp2.get(i);
			op = new ScanOperator(new File(Interpreter.getTemp() + "dummy " + (index-1)));
			op = new EXSortOperator(op,dummy1,location,pages,index*2-2);
			SelectOperator dummy = map.get(array[index]);
			MoveBack ex2 = null;
			ex2 = new EXSortOperator(dummy,dummy2,dummy.getLocation(),pages,index*2-1);
			file = new File(Interpreter.getTemp() + "dummy " + index);
			merge(op,ex2,dummy1,dummy2,location,dummy.getLocation());
			op = new ScanOperator(file);
		}
	}

	@Override
	public void visit(StringValue node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction node) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * visitor method for the and expression node.
	 * travel the right expression first since the tree I build is 
	 * left-heavy one. And the travel through the left one.
	 * @param node the and expression node to be visited.
	 */
	@Override
	public void visit(AndExpression node) {
		// TODO Auto-generated method stub
		node.getRightExpression().accept(this);
		node.getLeftExpression().accept(this);
	}

	@Override
	public void visit(OrExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between node) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * visiters of the equal method.
	 * simply traverse the tree nodes and let the column node
	 * to handle the rest of the work.
	 * @param the equals to node that shall be visited.
	 */
	@Override
	public void visit(EqualsTo node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
	}

	@Override
	public void visit(GreaterThan node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThanEquals node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThanEquals node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotEqualsTo node) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * visitor's method for the column node.
	 * put the column name on the array which will be used 
	 * as parameters for sorting.
	 * @param the column node to be visited.
	 */
	@Override
	public void visit(Column node) {
		// TODO Auto-generated method stub
		String str = node.getWholeColumnName();
		String[] dummy = str.split("\\.");
		if(marked.contains(dummy[0]))
			temp1.add(str);
		else temp2.add(str);
	}

	@Override
	public void visit(SubSelect node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor node) {
		// TODO Auto-generated method stub
		
	}

}
