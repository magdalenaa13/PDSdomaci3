
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class AvgTemp {
	public static class AvgTuple implements Writable {

        private int minimalnaSuma = 0;
        private int minCnt = 0;
        private int maksimalnaSuma = 0;
        private int maxCnt = 0;

        public int getMinimalnaSuma() {
            return minimalnaSuma;
        }

        public void setMinimalnaSuma(int minimalnaSuma) {
            this.minimalnaSuma = minimalnaSuma;
        }

        public int getMinCnt() {
            return minCnt;
        }

        public void setMinCnt(int minCnt) {
            this.minCnt = minCnt;
        }

        public int getMaksimalnaSuma() {
            return maksimalnaSuma;
        }

        public void setMaksimalnaSuma(int maksimalnaSuma) {
            this.maksimalnaSuma = maksimalnaSuma;
        }

        public int getMaxCnt() {
            return maxCnt;
        }

        public void setMaxCnt(int maxCnt) {
            this.maxCnt = maxCnt;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            minimalnaSuma = in.readInt();
            minCnt = in.readInt();
            maksimalnaSuma = in.readInt();
            maxCnt = in.readInt();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(minimalnaSuma);
            out.writeInt(minCnt);
            out.writeInt(maksimalnaSuma);
            out.writeInt(maxCnt);
        }

        public String toString() {
            return "MinAvg: " + (1.0 * minimalnaSuma/minCnt) + ", MaxAvg: " + (1.0 * maksimalnaSuma/maxCnt);
        }

    }


    public static class TempMapper extends Mapper<Object, Text, Text, AvgTuple> {
        private Text mesec = new Text();
        private AvgTuple outTuple = new AvgTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            mesec.set(line[1].substring(4,6));
            int temperatura = Integer.parseInt(line[3]);

            //TMAX or TMIN
            String extreme = line[2];
            if(extreme.equals("TMIN")){
                outTuple.setMinimalnaSuma(temperatura);
                outTuple.setMinCnt(1);
            }else if(extreme.equals("TMAX")){
                outTuple.setMaksimalnaSuma(temperatura);
                outTuple.setMaxCnt(1);
            }

            context.write(mesec, outTuple);
        }
    }

    public static class TempReducer extends Reducer<Text, AvgTuple, Text, AvgTuple> {

        private AvgTuple resultTuple = new AvgTuple();

        public void reduce(Text key, Iterable<AvgTuple> tuples, Context context) throws IOException, InterruptedException {
            int minSum = 0;
            int maxSum = 0;
            int minCount = 0;
            int maxCount = 0;

            for(AvgTuple tup : tuples){
                minSum += tup.getSumMin();
                maxSum += tup.getSumMax();
                minCount += tup.getMinCnt();
                maxCount += tup.getMaxCnt();
            }

            resultTuple.setSumMin(minSum);
            resultTuple.setMinCnt(minCount);
            resultTuple.setSumMax(maxSum);
            resultTuple.setMaxCnt(maxCount);

            context.write(key, resultTuple);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average extreme temperature");
        job.setJarByClass(AvgTemp.class);
        job.setMapperClass(TempMapper.class);
        job.setCombinerClass(TempReducer.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job,  new Path(args[2]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }

}
