import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MRDriver {

    public static void main(String[] args) throws Exception{


        Configuration c=new Configuration();

        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        //input file path
        Path input=new Path(files[0]);
        //output location
        Path output=new Path(files[1]);
        // set type of job to be performed from command line argument
        c.set("jobname",files[2]);
        //set job name
        Job j=new Job(c,"Sales Analytics");

        //set driver class
        j.setJarByClass(MRDriver.class);
        //set mapper class
        j.setMapperClass(MapperClass.class);
        //set mapper output key class
        j.setMapOutputKeyClass(Text.class);
        //set mapper output value class
        j.setMapOutputValueClass(DoubleWritable.class);

        //set reducer class
        j.setReducerClass(ReducerClass.class);
        //set reducer output key class
        j.setOutputKeyClass(Text.class);
        //set reducer output value class
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true)?0:1);


    }
}