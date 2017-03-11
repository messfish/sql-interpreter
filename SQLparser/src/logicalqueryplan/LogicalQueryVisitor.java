package logicalqueryplan;

/**
 * This interface is the logical query visitor. It uses the visitor's pattern.
 * @author messfish
 *
 */
public interface LogicalQueryVisitor {

	void visit(JoinOperators operator, String s, StringBuilder str);
	
	void visit(DuplicateEliminationOperators operator, String s, StringBuilder str);
	
	void visit(SortOperators operator, String s, StringBuilder str);
	
	void visit(SelectOperators operator, String s, StringBuilder str);
	
	void visit(ProjectOperators operator, String s, StringBuilder str);
	
}
