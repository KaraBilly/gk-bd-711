package phoneData;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowBeanReduce extends Reducer<LongWritable, FlowBean,LongWritable,FlowBean> {
    @Override
    public void reduce(LongWritable key,Iterable<FlowBean> values,Context context) throws IOException,InterruptedException{
        FlowBean sum = new FlowBean(0,0);
        for(FlowBean single: values){
            sum.accumulate(single.getUpFlow(),single.getDownFlow());
        }
        context.write(key,sum);
    }
}
