package logicalqueryplan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;

import logicalqueryplan.UnionFind;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import cs4321.operator.PhysicalPlanBuilder;


/**
 * this is the logical join operator, it build a tree which has multiple logical
 * selection operator as their children.
 * @author jz699 JUNCHEN ZHAN
 *
 */
public class JoinOperators extends Operators {
	
	private SelectOperators[] children; // this is the list of Selection operators.
	private UnionFind uf; // the union find data structures that is usable.
	private Set<String> visited; // the hashset that marks the visited table attribute.
	private String[] collection; // the collection of unused tuple evaluations.
	private List<List<String>> list; // the collection of PlainSelect for each table.
	private Map<String, Integer> index; // maps the table with their specific index in the array.
	
	/**
	 * constructor: this constructor help us to build the optimized query plan. 
	 * @param array the list of table names.
	 * @param ps the plain select that shall be used for restructuring.
	 * @param hash the hash table connects the table name with their scheme name.
	 */
	public JoinOperators(String[] array, PlainSelect ps, Map<String,String> hash) {
		children = new SelectOperators[array.length];
		index = new HashMap<>();
		for(int i=0;i<array.length;i++)
			index.put(array[i],i);
		list = new ArrayList<>(array.length);
		for(int i=0;i<array.length;i++)
			list.add(new ArrayList<String>());
		Expression exp = ps.getWhere();
		// get all the selections first.
		uf = new UnionFind(array,hash);
		visited = new HashSet<>();
		if(exp!=null){
			String[] str = exp.toString().split("AND");
			boolean[] marked = new boolean[str.length];
			for(int i=0;i<str.length;i++)
				marked[i] = buildUnion(str[i]);
			setUnion();
			for(int i=0;i<str.length;i++){
				if(!marked[i])
					marked[i] = build(str[i]);
			}
			for(int i=0;i<str.length;i++)
				checkValid(str[i], marked, i);
			List<String> strlist = new ArrayList<>();
			for(int i=0;i<str.length;i++){
				if(!marked[i]){
					strlist.add(str[i].trim());
				}
			}
			collection = new String[strlist.size()];
			for(int i=0;i<collection.length;i++)
				collection[i] = strlist.get(i);
		}else collection = new String[0];
		for(int i=0;i<children.length;i++)
			children[i] = new SelectOperators(array[i],list.get(i),hash,uf);
	}
	
	/**
	 * method for accepting visitor. just calls back visitor, logic of traversal
	 * will be handled in visitor method
	 * @param visitor visitor to be accepted.
	 * @param s the string that is used for indicating the tree depth.
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void accept(PhysicalPlanBuilder visitor, String s, StringBuilder str) {
		// TODO Auto-generated method stub
		visitor.visit(this,s,str);
	}

	/**
	 * this method returns the SelectOperators at the specific index.
	 * @param index the index of the SelectOperators
	 * @return the specific SelectOperators
	 */
	public SelectOperators getChild(int index){
		return children[index];
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
	 * this method set the union data structure. It traverse the whole
	 * column names and see whether it is visited and it is a root.
	 * if yes, add it to the root map. If not, add it to the arraylist
	 * which is in the Mule structure and is the value of the root as the key.
	 */
	private void setUnion(){
		Map<String, String> buff = uf.getMap();
		for(Map.Entry<String, String> entry : buff.entrySet()){
			if(visited.contains(entry.getKey())){
				if(entry.getValue().equals(" ")){
					if(!uf.root.containsKey(entry.getKey()))
						uf.root.put(entry.getKey(), new Mule());
					uf.root.get(entry.getKey()).list.add(entry.getKey());
				}else{
					String root = uf.findRoot(entry.getKey());
					if(!uf.root.containsKey(root))
						uf.root.put(root, new Mule());
					uf.root.get(root).list.add(entry.getKey());
				}
			}
		}
	}
	
	/**
	 * build the union from the given string.
	 * If this is an equation and the two parts of the equation are not numbers.
	 * push this into the union find data structure.
	 * @param s the string that shall be evaluated.
	 * @return whether this string is modified.
	 */
	private boolean buildUnion(String s){
		String[] dummy = s.trim().split("\\s+");
		if(dummy[1].equals("=")&&!isNumber(dummy[0])&&!isNumber(dummy[2])){
			String str1 = uf.findRoot(dummy[0]);
			String str2 = uf.findRoot(dummy[2]);
			if(!str1.equals(str2)) uf.Set(str1, str2);
			visited.add(dummy[0]);
			visited.add(dummy[2]);
			return true;
		}
		return false;
	}
	
	/**
	 * build the union data structure of the string s.
	 * if we find one of them is a number, 
	 * @param s the string that shall be evaluated.
	 * @return whether this string is modified.
	 */
	private boolean build(String s){
		String[] dummy = s.trim().split("\\s+");
		if(!dummy[1].equals("<>")&&(isNumber(dummy[0])||isNumber(dummy[2]))){
			String str = null;
			Mule mule = null;
			if(isNumber(dummy[2])){
				str = uf.findRoot(dummy[0]);
				mule = uf.getMule(str);
				if(mule==null){
					mule = new Mule();
					uf.root.put(str, mule);
					mule.list.add(dummy[0]);
				}
				Integer temp = Integer.parseInt(dummy[2]);
				if(dummy[1].equals("=")) uf.setEqual(mule,temp);
				else if(dummy[1].equals(">=")) uf.setLower(mule, temp);
				else if(dummy[1].equals(">")) uf.setLower(mule, temp+1);
				else if(dummy[1].equals("<=")) uf.setHigher(mule, temp);
				else if(dummy[1].equals("<")) uf.setHigher(mule, temp-1);
			}else{
				str = uf.findRoot(dummy[2]);
				mule = uf.getMule(str);
				if(mule==null){
					mule = new Mule();
					uf.root.put(str, mule);
					mule.list.add(dummy[2]);
				}
				Integer temp = Integer.parseInt(dummy[0]);
				if(dummy[1].equals("=")) uf.setEqual(mule,temp);
				else if(dummy[1].equals("<=")) uf.setLower(mule, temp);
				else if(dummy[1].equals("<")) uf.setLower(mule, temp+1);
				else if(dummy[1].equals(">=")) uf.setHigher(mule, temp);
				else if(dummy[1].equals(">")) uf.setHigher(mule, temp-1);
			}
			return true;
		}
		return false;
	}

	/**
	 * this method checks whether the string is valid to be put in the selection.
	 * check all the evaluation that the operation is not a join. Also it is not involved
	 * in the two methods above.
	 * @param str the evaluation sectence.
	 * @param marked the array indicates whether a specific element is chosen.
	 * @param index the index that indicates where the string is in.
	 */
	private void checkValid(String str, boolean[] marked, int index) {
		String[] dummy = str.trim().split("\\s+");
		String[] temp1 = dummy[0].split("\\.");
		String[] temp2 = dummy[2].split("\\.");
		if(temp1[0].equals(temp2[0])){
			int point = this.index.get(temp1[0]);
			list.get(point).add(str.trim());
			marked[index] = true;
		}
		else if((isNumber(dummy[0])||isNumber(dummy[2]))&&dummy[1].equals("<>")){
			int point = this.index.get(temp1[0]);
			list.get(point).add(str.trim());
			marked[index] = true;
		}
	}
	
	/**
	 * return the root of a given string.
	 * @param s the string as a parameter.
	 * @return the root of this string in the union find data structure.
	 */
	public String findRoot(String s) {
		return uf.findRoot(s);
	}
	
	/**
	 * return the union data structure.
	 * @return the union data structure.
	 */
	public Map<String, Mule> getUnion(){
		return uf.root;
	}
	
	/**
	 * return the union tree data structure.
	 * @return the union tree data structure.
	 */
	public Map<String, String> getRoot(){
		return uf.union;
	}
	
	/**
	 * return the list of string that is a join condition and is not included in union.
	 * @return the list of string that is not chosen in the previous methods.
	 */
	public String[] getCollection(){
		return collection;
	}
	
	/**
	 * this is the print method that is used for generating the logical query plan.
	 * @param String the inital String that will be used for printing. 
	 * @param str the string that will be used for writing the file.
	 */
	@Override
	public void print(String s, StringBuilder str) {
		// TODO Auto-generated method stub
		if(children.length!=1){
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			for(int i=0;i<collection.length-1;i++)
				sb.append(collection[i]).append(" AND ");
			if(collection.length!=0)
				sb.append(collection[collection.length-1]);
			sb.append("]");
			str.append(s+"Join"+sb.toString()).append("\n");
			for(Map.Entry<String, Mule> entry: uf.root.entrySet()){
				StringBuilder sb1 = new StringBuilder();
				sb1.append("[");
				Mule mule = entry.getValue();
				sb1.append("[");
				for(String str1:mule.list)
					sb1.append(str1).append(", ");
				sb1.deleteCharAt(sb1.length()-1);
				sb1.deleteCharAt(sb1.length()-1);
				sb1.append("]").append(", ");
				sb1.append("equals ").append(mule.ec).append(", ");
				sb1.append("min ").append(mule.lb).append(", ");
				sb1.append("max ").append(mule.up).append("]");
				str.append(sb1.toString()).append("\n");
			}
			for(int i=0;i<children.length;i++)
				this.getChild(i).print(s+"-",str);
		}else this.getChild(0).print(s,str);
	}
	
}
