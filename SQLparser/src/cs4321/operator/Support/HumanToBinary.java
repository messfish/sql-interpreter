package Support;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import SmallSQLServer.Main;

/**
 * This class is used to convert the human readable file into
 * binary file. Note the format of the binary file should be like this:
 * The header of the file should contains a byte includes how many tables
 * are there in this file. Followed by the name of the table, the name
 * of the attribute and the type of the attribute. Notice for the string
 * variable, we need to append a byte of the number that indicates the 
 * length of the string before the string.
 * Next is the content of the file. At first is a 4 byte value that shows
 * how many tuples are there in the page. Next is the content of the tuple:
 * The first is the occurence of this tuple. Followed by that is the content
 * of each single table, the first one is the order of that table, followed
 * by the data of that table. Notice there should be a byte indicates the 
 * length of the string if the data type is a string.
 * @author messfish
 *
 */
public class HumanToBinary {
	
	private static final int NUM_OF_BYTES = 16384;
	// this is the number of bytes in a single page. Notice I set it
	// to 16KB, so this is the number of bytes for that size.
	private Catalog catalog = Catalog.getInstance();
	
	/**
	 * This method is the main method that convert the human readable
	 * file into the binary readable file. Notice the format of the file
	 * should be followed from the definition of the class.
	 * @param file the human readable file.
	 * @param title the name of the table that needs to be converted.
	 * @return the file in the binary form.
	 */
	public File convert(File file, String title) {
		File result = new File(Main.getTest()+"/conversiontest/"+title+".b");
		int[] typelist = getType(title);
		try {
			FileOutputStream out = new FileOutputStream(result);
			FileChannel fc = out.getChannel();
			BufferedReader read = new BufferedReader(new FileReader(file));
			String str = read.readLine();
			String[] array = str.split("\\s+");
			ByteBuffer buffer = writeHead(array[0], typelist);
			fc.write(buffer);
			StringBuilder sb = new StringBuilder();
			while(true) {
				buffer = writePage(read, sb, typelist);
				if(sb.length()==0) break;
			}
			read.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * This method gets the name of the title and returns an array
	 * that stores the type of the elements.
	 * @param title the name of the table.
	 * @return an array of types.
	 */
	private int[] getType(String title) {
		Map<String, Mule> map = catalog.getSchema(title);
		int[] result = new int[map.size()];
		for(Map.Entry<String, Mule> entry : map.entrySet()) {
			Mule mule = entry.getValue();
			result[mule.getIndex()] = mule.getDataType();
		}
		return result;
	}

	/**
	 * This method is used to write the head of the file. Notice it should
	 * follow the format that is defined in the class.
	 * @param title the name of the table.
	 * @param typelist the array of data types.
	 * @return the byte buffer which stores the data.
	 */
	private ByteBuffer writeHead(String title, int[] typelist) {
		ByteBuffer result = ByteBuffer.allocate(NUM_OF_BYTES);
		result.put((byte)1);
		int index = 1;
		List<String> list = catalog.getAttributesList(title);
		result.put(index, (byte)title.length());
		index++;
		for(char c : title.toCharArray()) {
			result.put(index, (byte)c);
			index++;
		}
		for(int i=0;i<list.size();i++) {
			String str = list.get(i);
			result.put(index, (byte)str.length());
			for(char c : str.toCharArray()) {
				result.put(index, (byte)c);
				index++;
			}
			result.put(index, (byte)typelist[i]);
			index++;
		}
		return result;
	}
	
	/**
	 * This method is mainly used for writing the main page of the binary file.
	 * Notice it should follow the format defined in the class definition.
	 * @param read the buffered reader to get lines out from human file.
	 * @param sb the string that stores a temporary line.
	 * @param typelist the array that stores the type of the array.
	 * @return the byte buffer that contains the data written.
	 */
	private ByteBuffer writePage(BufferedReader read, StringBuilder sb,
								 int[] typeList) {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		return buffer;
	}
	
	/**
	 * This method is used to write the line in the buffer page. Notice
	 * it should follow the method in the class definition. If the 
	 * byte buffer could not handle that line, return null.
	 * @param index the starting point of the byte buffer page.
	 * @param s the string that needs to be parsed.
	 * @param typeList the array that stores the type of data.
	 * @return an array list storing bytes, null means we have an overflow.
	 */
	private List<Byte> writeLine(int index, String s, int[] typeList) {
		List<Byte> result = new ArrayList<>();
		return result;
	}
	
}
