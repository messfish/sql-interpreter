package Support;

/**
 * This class is mainly used to deal with the conversion between numeric
 * data to date value and time value.
 * @author messfish
 *
 */
public class TimeConversion {

	private int[] yearcount;
	// this variable is used to count the number of days in a year, 
	// the array length is 400 to represent a single cycle.
	private int[] datecount = {31,28,31,30,31,30,31,31,30,31,30,31};
	// this variable is used to count the number of days in a month.
	
	/**
	 * Constructor: this constructor is used to put all the data lists
	 * that will become handy in writing the methods below.
	 */
	public TimeConversion() {
		yearcount = new int[400];
		yearcount[0] = 366;
		/* the leap year is a number that could be divided by 4.
		 * Also the number that could be divided by both 400 and 
		 * 100, that means number cannot be divided by 400 but 
		 * could be divided by 100 is not considered as the leap year. */
		for(int i=1;i<400;i++) {
			if(i%4==0&&i%100!=0) yearcount[i] = 366;
			else yearcount[i] = 365;
		}
	}
	
	/**
	 * This function is used to convert the numeric data into the 
	 * date format. Notice we need to consider the definition of 
	 * the leap year, also, the valid date should not be lower
	 * than 0000/00/00 or larger than 9999/12/31. 
	 * @param data the data needs to be transformed to string.
	 * @return the date in the format as %%%%/%%/%%.
	 */
	public String fromNumberToDate(double data) {
		int number = (int)data, base = 0, rest = 0, index = 0;
		int period = 400 * 365 + 97;
		base = 400 * (number / period);
		rest = number % period;
		while(rest - yearcount[index] >= 0) {
			rest -= yearcount[index];
			index++;
		}
		int year = base + index;
		if(yearcount[index]==366) datecount[1] = 29;
		index = 0;
		while(rest - datecount[index] >= 0) {
			rest -= datecount[index];
			index++;
		}
		int month = index + 1, day = rest + 1;
		String result = String.format("%04d", year) + "/" +
						String.format("%02d", month) + "/" +
						String.format("%02d", day);
		/* do not forget to reset the number of date in Feb. back to 28! */
		datecount[1] = 28;
		return result;
	}

	/**
	 * this function is used to convert the numeric data into
	 * a string in a time format. Notice the format should be 
	 * like this: %%:%%:%%
	 * @param data the data needs to be transformed to string.
	 * @return the time in the format as %%:%%:%%
	 */
	public String fromNumberToTime(double data) {
		int number = (int)data, hour = 0, minute = 0, second = 0;
		hour = number / 3600;
		number = number % 3600;
		minute = number / 60;
		second = number % 60;
		String result = String.format("%02d", hour) + ":" +
						String.format("%02d", minute) + ":" +
						String.format("%02d", second);
		return result;
	}

	/**
	 * This method is used to convert a date to a numeric value.
	 * Notice the hierarchy should be like this: the day has the lowest 
	 * level, followed by the month, the year is the highest.
	 * @param data the date in the format %%%%/%%/%%
	 * @return the numeric transformation of the date.
	 */
	public double fromDateToNumber(String data) {
		int period = 400 * 365 + 97;
		String[] array = data.split("/");
		int[] dummy = new int[3];
		for(int i=0;i<3;i++)
			dummy[i] = Integer.parseInt(array[i]);
		int result = period * (dummy[0] / 400), rest = dummy[0] % 400;
		for(int i=0;i<rest;i++)
			result += yearcount[i];
		if(rest==0||(rest%4==0&&rest%100!=0))
			datecount[1] = 29;
		for(int i=1;i<dummy[1];i++)
			result += datecount[i - 1];
		result += dummy[2] - 1;
		/* do not forget to reset the number of date in Feb. back to 28! */
		datecount[1] = 28;
		return result;
	}

	/**
	 * This method is used to convert a time to a numeric value.
	 * Notice the hierarchy should be like this: the second has the lowest
	 * level, followed by the minute, the hour is the highest.
	 * @param data the time in the format %%:%%:%%
	 * @return the numeric transformation of the time.
	 */
	public double fromTimeToNumber(String data) {
		String[] array = data.split(":");
		int[] dummy = new int[3];
		for(int i=0;i<3;i++)
			dummy[i] = Integer.parseInt(array[i]);
		int result = dummy[0] * 3600 + dummy[1] * 60 + dummy[2];
		return result;
	}

}
