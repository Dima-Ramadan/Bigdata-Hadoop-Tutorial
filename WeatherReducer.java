/* This is the reducer in Historical Weather multi-step mapreduce job.  */

/* The main aim of this class is to find the average temperature per month for the location entered by user in the command line */
/* This class will take the output of WeatherDateMapper (Text, DoubleWritable) ==> [(YYYY - MM , Location), MaxTemp] 
and will emit ((date[YYYY - MM],Location), Average Temp) */


package org.myorg;
// Importing the libraries that contain the classes and methods needed in the Reducer class
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	 /*@override is used to change the original behaviour of the method “reduce” in the
	 parent Class “Reducer” */
	 @Override
	 /* the reduce method has three parameters the input Text Key, and the Array of DoubleWritable values, 
	 and the context to move data between hadoop mapreduce components.*/
	 public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
	 throws IOException, InterruptedException
	 {
		 /*create a double variable called TempSum and initialise it with 0. used to store the summation of all 
		 temperature values for each key*/
		 
		 /* create an integer variable called count and initiated with zero, will be used to store the number of 
		  * temperature records for each key (values count for each key) */
		double TempSum = 0.0;
		int count = 0 ; 
		
		//Create Iterator object that iterates through the values and save the result to DoubleWritable iterator 
		Iterator<DoubleWritable> iterator = values.iterator();
		
		//check if there is at least one item left to iterate over using .hasNext() method.
			while (iterator.hasNext()) {
			//iterate through the values in order -not randomly- using next() method and extract the value in the iterator (temp) 
			// the value of temp in the current iteration to TempSum.  
				TempSum += iterator.next().get();
				count = count + 1;  //add 2 to the number of counts in each iteration 
			}
		//divide the total sum (TempSum) by the number of counts to find the mean temp for each month in the years between(2005-2021)
			double meanTemp = TempSum / count; 
		//emit Text key, and DoubleWritable value => [(date, location), meanMaxTemp]
		context.write(key, new DoubleWritable(meanTemp));
	 }
}


