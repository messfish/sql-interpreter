package cs4321.project2;

import java.util.Map;
import java.util.Stack;

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
 * tests whether an expression is true or not. Post traverse the tree, use 
 * two stacks to store integers and boolean values. When there is a comparison,
 * pop out two integers and compare them, store the value on the boolean stack;
 * when there is a "AND" expression, pop out two boolean values and compare them,
 * store the value on the boolean stack.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class Evaluation implements ExpressionVisitor {

	private Stack<Long> stack1; // first stack, stores the long integers.
	private Stack<Boolean> stack2; // second stack, stores the booleans for logic equations.
	private Tuple tuple; // the tuple that is referenced.
	private Catalog catalog; // the catalog needs to be referenced.
	private Map<String,String> hash;
	
	/**
	 * constructor, assigns the field to the arguments.
	 * @param tuple the tuple that will be examined.
	 * @param hash the hash map connects the table to their aliases.
	 */
	public Evaluation(Tuple tuple, Map<String,String> hash){
		stack1 = new Stack<>();
		stack2 = new Stack<>();
		catalog = Catalog.getInstance();
		this.tuple = tuple;
		this.hash = hash;
	}
	
	/**
	 * check whether the evaluation is true or not.
	 * @return the result "This evaluation is valid."
	 */
	public boolean getResult(){
		if(stack2.isEmpty()) return true;
		return stack2.peek();
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
	 * visit method for the long integers.
	 * push the long integer on stack1.
	 * @param the long value node.
	 */
	@Override
	public void visit(LongValue node) {
		// TODO Auto-generated method stub
		stack1.push(node.getValue());
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
	 * this method is used to deal with parenthesis,
	 * we just get the expression inside the parenthesis
	 * and recursively calls the expression.
	 */
	@Override
	public void visit(Parenthesis node) {
		// TODO Auto-generated method stub
		node.getExpression().accept(this);
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
	 * Visit method for the and expression node. 
	 * post traverse the node, pop two boolean values from the 
	 * stack2 and make a and logic comparison. Push the result on stack2.
	 * @param an and expression node.
	 */
	@Override
	public void visit(AndExpression node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		boolean temp1 = stack2.pop(), temp2 = stack2.pop();
		stack2.push(temp1&temp2);
	}

	/**
	 * Visit method for the or expression node. 
	 * post traverse the node, pop two boolean values from the 
	 * stack2 and make a or logic comparison. Push the result on stack2.
	 * @param an or expression node.
	 */
	@Override
	public void visit(OrExpression node) {
		// TODO Auto-generated method stub
		node.getLeftExpression().accept(this);
		node.getRightExpression().accept(this);
		boolean temp1 = stack2.pop(), temp2 = stack2.pop();
		stack2.push(temp1|temp2);
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
		long temp1 = stack1.pop(), temp2 = stack1.pop();
		stack2.push(temp1==temp2);
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
		long temp1 = stack1.pop(), temp2 = stack1.pop();
		stack2.push(temp2>temp1);
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
		long temp1 = stack1.pop(), temp2 = stack1.pop();
		stack2.push(temp2>=temp1);
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
		long temp1 = stack1.pop(), temp2 = stack1.pop();
		stack2.push(temp2<temp1);
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
		long temp1 = stack1.pop(), temp2 = stack1.pop();
		stack2.push(temp2<=temp1);
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
		long temp1 = stack1.pop(), temp2 = stack1.pop();
		stack2.push(temp1!=temp2);
	}

	/**
	 * visit method for the column node.
	 * get the column name from the node, use the map from the catalog
	 * to get the desired tuples we want and push it onto the stack1.
	 * @param node the column node.
	 */
	@Override
	public void visit(Column node) {
		// TODO Auto-generated method stub
		String s = node.getWholeColumnName();
		String[] temp = s.split("\\.");
		s = hash.get(temp[0]) + "." + temp[1];
		int index = catalog.getColumn(s);
		stack1.push((long)tuple.getData(index));
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
