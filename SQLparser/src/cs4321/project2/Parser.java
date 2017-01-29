package cs4321.project2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import logicalqueryplan.DuplicateEliminationOperators;
import logicalqueryplan.JoinOperators;
import logicalqueryplan.Operators;
import logicalqueryplan.ProjectOperators;
import logicalqueryplan.SortOperators;
import cs4321.operator.PhysicalPlanBuilder;
import cs4321.support.Catalog;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This class builds the logical query plan, take out the aliases and store them
 * in a hash map. get out the table names and plain select language and send them
 * to the physical plan builder. Print out the logical and physical query plan and 
 * spit out the result tuples.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class Parser {
	
	private static Map<String, String> map; 
	// the map connects the table name to their schema name.
	private static Operators ops; // the super class of logical operators.
	
	/**
	 * handle the plain select language and construct the physical and logical query
	 * plan to handle the result. Print the logical and physical plan out. 
	 * @param ps the plainSelect needed to be parsed.
	 * @param s the string shows the output directory.
	 * @param index the index of the output files.
	 */
	public static void handle(PlainSelect ps, String s, int index){
		map = new HashMap<>();
		String str = ps.getFromItem().toString();
		String[] orz = str.split("\\s+");
		if(orz.length==3) map.put(orz[2], orz[0]);
		else map.put(orz[0], orz[0]);
		@SuppressWarnings("rawtypes")
		List list = ps.getJoins();
		String[] temp = null;
		if(list!=null) temp = new String[list.size()+1];
		else temp = new String[1];
		String ab = ps.getFromItem().toString();
		String[] ss = ab.split("\\s+");
		temp[0] = ss[ss.length-1];
		for(int i=1;i<temp.length;i++){
			String abc = list.get(i-1).toString();
			String[] sss = abc.split("\\s+");
			temp[i] = sss[sss.length-1];
			if(sss.length==3) map.put(sss[2], sss[0]);
			else map.put(sss[0], sss[0]);
		}
		ops = new JoinOperators(temp,ps,map);
		ops = new ProjectOperators(ops,ps);
		if(ps.getOrderByElements()!=null)
			ops = new SortOperators(ops,ps);
		if(ps.getDistinct()!=null){
			if(!(ops instanceof SortOperators)) 
				ops = new SortOperators(ops,ps);
			ops = new DuplicateEliminationOperators(ops);
		}
		int[] previous = new int[temp.length];
		for(int i=1;i<previous.length;i++){
			String dummy = map.get(temp[i-1]);
			List<String> list1 = Catalog.getInstance().getList(dummy);
			previous[i] = previous[i-1] + list1.size();
		}
		StringBuilder sb = new StringBuilder();
		ops.print("",sb);
		writeFile("_logicalplan",sb,index);
		PhysicalPlanBuilder ppb = new PhysicalPlanBuilder(temp,ps,map,previous);
		StringBuilder sb1 = new StringBuilder();
	    ops.accept(ppb,"",sb1);
	    writeFile("_physicalplan",sb1,index);
		ppb.dump(s+"/query",String.valueOf(index));
	}
	
	/**
	 * this method writes the plan builder into the file.
	 * @param s tells whether this is a logical plan or a physical plan.
	 * @param sb the string builder contains the content of the plan builder.
	 * @param index tells the order of this query.
	 */
	private static void writeFile(String s, StringBuilder sb, int index) {
		File file = new File(Interpreter.getOutput()+"/query"+index+s);
		try {
			BufferedWriter buffer = new BufferedWriter(new FileWriter(file));
			buffer.write(sb.toString());
			buffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
