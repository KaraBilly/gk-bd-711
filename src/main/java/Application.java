
import javafx.scene.layout.FlowPane;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
        System.out.printf("hello");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowBean.class);
        job.setMapperClass(FlowBeanMap.class);
        job.setReducerClass(FlowBeanReduce.class);

        job.setMapOutputKeyClass(String.class);
        job.setMapOutputKeyClass(FlowBean.class);

        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.addInputPath((JobConf) job.getConfiguration(),new Path("C:\\Users\\43121\\Documents\\JavaWorkSpace\\gk-bd-711\\src\\main\\java\\source\\HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(),new Path("C:\\Users\\43121\\Documents\\JavaWorkSpace\\gk-bd-711\\src\\main\\java\\source\\output.text"));

        System.exit(job.waitForCompletion(true) ? 0 :1);
    }

    private static List<String> getData(String fileName) throws IOException{
        List<String> res = new ArrayList<>();
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //System.out.println("line " + line + ": " + tempString);
                res.add(tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return res;
    }

}