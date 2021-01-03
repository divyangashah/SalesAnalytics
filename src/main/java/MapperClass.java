import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapperClass extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, DoubleWritable> { // org.apache.hadoop.mapred.MapperClass<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    private LongWritable count = new LongWritable(1);
    private DoubleWritable avg = new DoubleWritable(0);
    private DoubleWritable variance = new DoubleWritable(0);
    private DoubleWritable sum = new DoubleWritable(0);
    private DoubleWritable min = new DoubleWritable(Double.MAX_VALUE);
    private DoubleWritable max = new DoubleWritable(Double.MIN_VALUE);
    private List<Double> values = new ArrayList<Double>();
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
        if (key.get() == 0)
            return;
        // get job type based on which we decide which task need to be performed
        String jobType =  con.getConfiguration().get("jobname");
        String groupBy = "";

        if(jobType.equals("region")) {
            // select region for region wise profit
            groupBy = value.toString().split(",")[0];
        } else if(jobType.equals("country")){
            // select country for region wise profit
            groupBy = value.toString().split(",")[1];
        } else if(jobType.equals("itemType")){
            // select item type for region wise profit
            groupBy = value.toString().split(",")[2];
        } else if(jobType.equals("itemTypeChannelSales") || jobType.equals("itemTypeChannelProfit")){
            //channel based item type selection
            groupBy = value.toString().split(",")[2];
            groupBy += ":" + value.toString().split(",")[3];
        }
        double profit_or_sold = 0;
        if(jobType.equals("itemTypeChannelSales")){
            // get channel based itemtype sold
            profit_or_sold = Double.parseDouble(value.toString().split(",")[8]);
        } else {
            // get profit values for region/country/item type
            profit_or_sold = Double.parseDouble(value.toString().split(",")[13].replace("\\",""));
        }

        con.write(new Text(groupBy), new DoubleWritable(profit_or_sold));

    }
}