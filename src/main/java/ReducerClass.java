import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {


			Iterator<DoubleWritable> valuesIt = values.iterator();
			double profit = 0;
			while (valuesIt.hasNext()){
				profit += valuesIt.next().get();

			}
			// calculate sum of each operations mentioned in mapper
			context.write(key,new DoubleWritable(profit));
	}	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		super.cleanup(context);

	}
}
