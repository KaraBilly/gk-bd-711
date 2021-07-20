
import javafx.scene.layout.FlowPane;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import phoneData.FlowBean;
import phoneData.FlowBeanMap;
import phoneData.FlowBeanReduce;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Application {
    public static void main(String[] args) throws Exception{
        //System.out.println("Hello World");
//        try {
//            List<String> s = getData("C:\\Users\\43121\\Documents\\JavaWorkSpace\\gk-bd-711\\src\\main\\java\\source\\HTTP_20130313143750.dat");
//            List<String> sfds = s;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        System.out.print("hello");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Application.class);
        job.setMapperClass(FlowBeanMap.class);
        job.setReducerClass(FlowBeanReduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths((JobConf) job.getConfiguration(),new Path(".\\src\\main\\java\\source\\HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(),new Path(".\\src\\main\\java\\source\\output"));

        System.exit(job.waitForCompletion(true) ? 0 :1);

        System.out.print("world");
    }

}