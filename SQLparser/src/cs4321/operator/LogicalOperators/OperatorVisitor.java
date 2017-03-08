package LogicalOperators;

/**
 * this interface stores all the visiting method of the
 * concrete operators.
 * @author messfish
 *
 */
public interface OperatorVisitor {

	void visit(OrderByOperators order);
	
	void visit(GroupByOperators group);
	
	void visit(ProjectOperators project);
	
	void visit(DistinctOperators distinct);
	
	void visit(JoinOperators join);
	
	void visit(HavingOperators having);
	
	void visit(SelectOperators select);
	
}
