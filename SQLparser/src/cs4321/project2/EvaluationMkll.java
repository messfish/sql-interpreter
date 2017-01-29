package cs4321.project2;

import java.util.Map;
import java.util.Stack;

import cs4321.operator.SelectOperator;
import cs4321.operator.Tuple;
import cs4321.support.Catalog;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
 * this class handles the created join trees. recursively check whether the 
 * set of tuples are valid and send the temporary result to the next table.
 * When this program ends, the result is the next valid tuple.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class EvaluationMkll implements ExpressionVisitor {

	protected Stack<Long> stack1; 
	protected Stack<Boolean> stack2;
	protected Map<String,SelectOperator> map;
	protected Map<String,Tuple> map2;
	protected Map<String,String> hash; 
	// this map links the aliases to their original tables.
	protected int length; // the index of the array.
	protected Catalog catalog;
	protected boolean stillLeft = true, isValid;
	// first boolean tells whether there are available tuples left.
	// next boolean controls the traverse of join tree.
	protected String[] array;
	
	/**
	 * constructor, assigns the field to the arguments.
	 * @param map the map connects the table with selectoperator.
	 * @param array the list of array
	 * @param map2 the map connects the table with their tuples.
	 * @param hash the map connects the table with their aliases.
	 */
	public EvaluationMkll(Map<String,SelectOperator> map, String[] array,
			                  Map<String,Tuple> map2, Map<String,String> hash) {
		this.map = map;
		this.array = array;
		this.map2 = map2;
		this.hash = hash;
		stack1 = new Stack<>();
		stack2 = new Stack<>();
		length = array.length - 1;
		catalog = Catalog.getInstance();
	}
	
	/** 
	 * check whether this tuple is a valid one.
	 * @return the state "this tuple is valid."
	 */
	public boolean isTuple(){
		if(stack2.isEmpty()) return true;
		return stack2.pop();
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

	/**
	 * method visit the long value.
	 * push a true value on the second stack since this node
	 * indicates a cross product.
	 */
	@Override
	public void visit(LongValue node) {
		// TODO Auto-generated method stub
		stack2.push(true);
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
	 * method visit the parenthesis node.
	 * get the next tuple of this node and check whether it is valid.
	 * if not, fetch the next tuple and check until there is a valid one.
	 * if we meet the end of the tuple, go to the table adjacent to this
	 * table and fetch the next valid tuple. Reset this table and continue 
	 * to search for the valid one.  
	 * @param the parenthesis node to be visited.
	 */
	@Override
	public void visit(Parenthesis node) {
		// TODO Auto-generated method stub
		// this check whether this is the start of getting the node.
		if(length==array.length-1&&map2.containsKey(array[length])) isValid = true;
		// length equals to 0 means we get the deepest join node.
        if(length==0){
        	map2.put(array[length],map.get(array[length]).getNextTuple());
        	// if we cannot get the next tuple, that means the search of 
        	// valid tuples is already done, set stillLeft to false.
        	if(map2.get(array[length])==null){
        		stillLeft = false;
        	}
        }else{
        	map2.put(array[length],map.get(array[length]).getNextTuple());
        	if(map2.get(array[length])!=null){
        	    length--;
        	    node.getExpression().accept(this);
        	    length++;
        	}
        	while((!isTuple()||map2.get(array[length])==null)&&stillLeft){
        		if(map2.get(array[length])!=null){
        		    map2.put(array[length], map.get(array[length]).getNextTuple());
        		}
        		isValid = true;
        		if(map2.get(array[length])!=null){
        		    node.getExpression().accept(this);
        		}else{
        			map.get(array[length]).reset();
        			map2.put(array[length], map.get(array[length]).getNextTuple());
        		    AndExpression ae = (AndExpression)node.getExpression();
        		    length--;
        		    // jump directly to the left parenthesis and traverse it.
        		    ae.getLeftExpression().accept(this);
        		    // when we finished searching for the next valid tuple of the 
        		    // sub join trees. traverse the right node to make sure there 
        		    // it is valid.
        		    node.getExpression().accept(this);
        		    length++;
        		}
        	}
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
	 * method that visit the and expression node.
	 * if isValid is true, only traverse the right part.
	 * else, make a post traverse of the tree.
	 */
	@Override
	public void visit(AndExpression node) {
		// TODO Auto-generated method stub
		// check whether we only need to traverse the right part.
		if(isValid){
			isValid = false; // set it to false to make a full traverse of right part.
			node.getRightExpression().accept(this);
			isValid = true; // set it back to true for future use.
		}
		else{
		    node.getLeftExpression().accept(this);
		    node.getRightExpression().accept(this);
		    // if we do not get the check, we may get an EmptyStackException
		    // since we do not push boolean values on the stack
		    // when the left node is a parenthesis node.
		    if(!(node.getLeftExpression() instanceof Parenthesis)){
		        boolean value1 = stack2.pop();
		        boolean value2 = stack2.pop();
		        stack2.push(value1&value2);
		    }
		}
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
	 * visit method for the equals to node.
	 * post traverse the node, pop two long values out of the stack1.
	 * check whether the two values are the same and push the result
	 * on the stack2.
	 * @param an equals to expression node.
	 */
	@Override
	public void visit(EqualsTo node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		long num1 = stack1.pop(), num2 = stack1.pop();
		stack2.push(num1==num2);
	}

	/**
	 * visit method for the greater than node.
	 * post traverse the node, pop two long values out of the stack1.
	 * check whether the second one popped is greater than the first
	 * one and push the result on the stack2.
	 * @param an greater expression node.
	 */
	@Override
	public void visit(GreaterThan node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		long num1 = stack1.pop(), num2 = stack1.pop();
		stack2.push(num2>num1);
	}

	/**
	 * visit method for the greater than equals node.
	 * post traverse the node, pop two long values out of the stack1.
	 * check whether the second one popped is greater than or equals 
	 * to the first one and push the result on the stack2.
	 * @param an greater than equals expression node.
	 */
	@Override
	public void visit(GreaterThanEquals node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		long num1 = stack1.pop(), num2 = stack1.pop();
		stack2.push(num2>=num1);
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

	/**
	 * visit method for the minor than node.
	 * post traverse the node, pop two long values out of the stack1.
	 * check whether the second one popped is minor than 
	 * to the first one and push the result on the stack2.
	 * @param a minor than expression node.
	 */
	@Override
	public void visit(MinorThan node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		long num1 = stack1.pop(), num2 = stack1.pop();
		stack2.push(num2<num1);
	}

	/**
	 * visit method for the minor than equals node.
	 * post traverse the node, pop two long values out of the stack1.
	 * check whether the second one popped is minor than equals
	 * to the first one and push the result on the stack2.
	 * @param a minor than equals expression node.
	 */
	@Override
	public void visit(MinorThanEquals node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		long num1 = stack1.pop(), num2 = stack1.pop();
		stack2.push(num2<=num1);
	}

	/**
	 * visit method for the equals to node.
	 * post traverse the node, pop two long values out of the stack1.
	 * check whether the two values are not the same and push the result
	 * on the stack2.
	 * @param an not equals to expression node.
	 */
	@Override
	public void visit(NotEqualsTo node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		long num1 = stack1.pop(), num2 = stack1.pop();
		stack2.push(num1!=num2);
	}

	/**
	 * visit method for the column node.
	 * get the respective column of the alias and push the result on the stack1.
	 * if we cannot find the tuple, this indicates there are no tuples in 
	 * this table. So we set stillLeft to false and push an artificial long value
	 * on the stack to avoid EmptyStackException. 
	 * @param the expression node.
	 */
	@Override
	public void visit(Column node) {
		// TODO Auto-generated method stub
		String str = node.getWholeColumnName();
		String[] dummy = str.split("\\.");
		str = hash.get(dummy[0]) + "." + dummy[1];
		// reset the string to make it available for searching in catalogs.
		String temp = node.getTable().getName();
		int index = catalog.getColumn(str);
		if(map2.get(temp)==null){
			stillLeft = false;
			stack1.push((long) 0);
			return;
		}
		stack1.push((long)map2.get(temp).getData(index));	
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
