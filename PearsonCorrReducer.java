package org.myorg;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PearsonCorrReducer extends Reducer<Text, Text, Text, DoubleWritable>
{
	//Declare the variables that will be used in the code
	 ArrayList<String> ValuesList = new ArrayList<String>();
	 Text AreaFireStrength = new Text();
	 @Override
	 public void reduce(Text key, Iterable<Text> values, Context context)
	 throws IOException, InterruptedException
	 {
		 //assuming that the left item in each value is x, and the right item is y
		 //breaking Pearson's correlation equation to tiny pieces and declare
		 //each piece as an integer variable
		 double x = 0.0;
		 double y = 0.0;
		 double xx = 0.0;
		 double yy = 0.0;
		 double xy = 0.0;
		 double count = 0.0;
		 double xSum = 0.0;
		 double ySum = 0.0;
		 double xxSum = 0.0;
		 double yySum = 0.0;
		 double xySum = 0.0;
		 
		 //iterate through the list of values 
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			Text value = iterator.next();
			//split each value on comma
			String []  Line  = value.toString().split(",");
			//CONVERTS THE VALUE TO DOUBLE data type
			 x = Double.parseDouble(Line[0].trim());
			 y = Double.parseDouble(Line[1].trim());
			 /* start to apply the equation's bits that needs summation for all the records in the variable */
			 xSum += x;  //sum the values for the first variable (ex: Temp)
			 ySum += y; //sum the values for the first variable (ex: Area)
			 
			 xx = x * x; //square each record in the first variable 
			 xxSum += xx; //Sum the squared records
			 
			 yy = y *y; //square each record in the second variable
			 yySum += yy; //Sum the squared records
			 
			 xy = x * y; //for each pair, multiply the records of the two variables
			 xySum += xy; //sum the multiplication results
			count = count + 1;  //find the number of records 
}
			//Continue with the equation's requirement (the parts that don't need to loop through )
			double meanXSumYSum = (xSum * ySum) / count; 
			double MeanxSumSqrt = Math.pow(xSum, 2.0) / count;
			double MeanySumSqrt = Math.pow(ySum, 2.0) / count;
			
			//numerator value
			double equationNumerator = xySum - meanXSumYSum;
			//denominator value
			double denominatorInner = (xxSum - MeanxSumSqrt) * (yySum - MeanySumSqrt);
			//the part under the square root 
			double equationDenominator = Math.pow(denominatorInner, 0.5);
			//divide the numerator on the denominator
			double pearsonCoef = equationNumerator / equationDenominator;
			
		context.write(key, new  DoubleWritable(pearsonCoef)); //output the indices and the coefficient
	 }
}

