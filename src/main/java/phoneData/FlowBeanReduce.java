package phoneData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    public void reduce(String key,Iterable<FlowBean> value,Reducer<String,FlowBean,String,FlowBean>.Context context) throws IOException,InterruptedException{
        FlowBean sum = new FlowBean(0,0);
        for(FlowBean single: value){
            sum.accumulate(single.getUpFlow(),single.getDownFlow());
        }
        context.write(key,sum);
    }
}
