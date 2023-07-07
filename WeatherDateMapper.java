package org.myorg;
//Import the libraries that will be used in Mapper Class 
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//The class's input is Text (key, value )pairs and the output Text Key, and DoubleWritable value.
public class WeatherDateMapper extends Mapper<Text, Text, Text, DoubleWritable>{
	//Declare hadoop variables that will be emitted from the class 
	
	private Text DateLocation = new Text();
	private DoubleWritable Temp= new DoubleWritable();
	 
	 /*@override is used to change the original behaviour of the method “map” in the parent
	 Class “Mapper” */
	 @Override
	 
	 // the map method has three parameters, the Test(key,value) pair. and the context for sending data in hadoop's framework
	 public void map(Text key, Text value, Context context)
	 throws IOException, InterruptedException
	 {
		 //Split each line on each comma (will separate the location and date) and save the results in String TempDate array.
		 String[] TempDate = value.toString().split(",");
		 //Extract the date which is index 0 of the array 
		 String DateEx = TempDate[0];
		 //Split the date on each (-) 
		 String[] DateSplit = DateEx.split("-");
		 // extract the values in index 0 which is the year and index 1 which is the month 
		 //then concatenate the year with "-" and the month. 
		 String DateYM = DateSplit[0] + "-" + DateSplit[1];

		 //extract the Location which is the input key, and convert it to string so we can add it to other java strings (date).
		 String Location = key.toString();
		 
		/* Concatenate the date (in years and months) to the location entered in the command line, seperated by comma and 
		 save the results to DateLoc string */
		 String DateLoc = DateYM + "," +  Location;
		 //set the Text variable DateLocation (hadoop variable) to the value of DateLoc because we want to emit it from the mapper
		 DateLocation.set(DateLoc);
		 
		 //Extract temperature string value from TempDate array and remove any white spaces then convert it to double,
		 // the result saved in MaxTemp.
		 double MaxTemp = Double.parseDouble(TempDate[1].trim());
		 //set the DoubleWritable variable (Temp) which is hadoop data type to the double variable (java data type) MaxTemp.
		 Temp.set(MaxTemp);

		 //emit the Text key Date Location and the DoubleWritable value Temp
				
		 context.write(DateLocation, Temp);
	 }
}



