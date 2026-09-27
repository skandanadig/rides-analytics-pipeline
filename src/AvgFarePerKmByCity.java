import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgFarePerKmByCity {

    public static class FareMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text cityOut = new Text();
        private final DoubleWritable farePerKmOut = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty() || line.startsWith("trip_id,")) {
                return;
            }

            try {
                String[] fields = line.split(",", -1);

                String city = fields[2].trim();
                double distanceKm = Double.parseDouble(fields[5].trim());
                double fare = Double.parseDouble(fields[6].trim());

                if (distanceKm <= 0) {
                    return;
                }

                cityOut.set(city);
                farePerKmOut.set(fare / distanceKm);
                context.write(cityOut, farePerKmOut);

            } catch (Exception ignored) {
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text city, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 0) {
                context.write(
                        city,
                        new Text(String.format(Locale.US, "%.4f", sum / count))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Fare Per Km by City");

        job.setJarByClass(AvgFarePerKmByCity.class);
        job.setMapperClass(FareMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
