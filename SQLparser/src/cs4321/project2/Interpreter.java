package cs4321.project2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * This class intends to become the interpreter of those queries.
 * It is the top class required by the project.
 * @author jz699, JUNCHEN ZHAN
 *
 */
public class Interpreter {
     
	private CCJSqlParser parser; // the file about the query.
	private static String inputdir = "";
	// this String is used for inputs.
	private static String outputdir = "";
    // this String is used for outputs.
	private static String tempdir = "/";
	// this String is used for temporary input.
	private static int index = 1;
	
	/**
	 * Constructor: create the parser by the expected query.
	 * @param query the location of the query File.
	 * @throws FileNotFoundException throws this exception out
	 * when the file is not found.
	 */
	public Interpreter(String query) throws FileNotFoundException{
		parser = new CCJSqlParser(new FileReader(query));
	}
	
	/**
	 * get the input directory as a string.
	 * @return the input directory.
	 */
	public static String getInput(){
		return inputdir;
	}
	
	/**
	 * get the output directory as a string.
	 * @return the output directory.
	 */
	public static String getOutput(){
		return outputdir;
	}
	
	/**
	 * get the temporary directory as a string.
	 * @return the temporary directory.
	 */
	public static String getTemp(){
		return tempdir;
	}
	
	/**
	 * get the next line of the queries and fetch the result.
	 * @param the index of the output file.
	 * @return the Statement to check whether it is null.
	 */
	public Statement getLine(int index){
		Statement state = null;
		try{
			if((state = parser.Statement())!=null){
				Select select = (Select) state;
				PlainSelect ps = (PlainSelect)select.getSelectBody();
				long start = System.currentTimeMillis();
				Parser.handle(ps,outputdir,index);
				long end = System.currentTimeMillis();
				System.out.println("The cost of query " + index + " is: " + (end-start));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return state;
	}
	
	/**
	 * get all the queries and deal with them at once.
	 */
	public void dump(){
		while(getLine(index)!=null){
			File dir = new File(Interpreter.getTemp());
			for(File file: dir.listFiles()){ 
			    if (!file.isDirectory())
			        file.delete();
			}
			index++;
		}
	}
	
	/**
	 * this is the main method that will be used.
	 * @param args the array stores the location of the file.
	 */
	public static void main(String[] args){
		File file = new File(args[0]);
		try{
			BufferedReader fread = new BufferedReader(new FileReader(file)); 
			inputdir = fread.readLine();
			outputdir = fread.readLine();;
			tempdir = fread.readLine() + tempdir;
			BTreeBuilder BTree = new BTreeBuilder();
			BTree.dump();
		    Interpreter ip = new Interpreter(inputdir + "/queries.sql");
		    ip.dump();
		    fread.close();
		}catch(Exception e){
			System.out.print("Files not found!");
		}
	}
	
}
