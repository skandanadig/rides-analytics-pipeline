import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

public class AvgSpeedByCity {

    public static class SpeedMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private static final DateTimeFormatter FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        private final Text cityOut = new Text();
        private final DoubleWritable speedOut = new DoubleWritable();

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

                LocalDateTime pickup =
                        LocalDateTime.parse(fields[3].trim(), FORMATTER);
                LocalDateTime dropoff =
                        LocalDateTime.parse(fields[4].trim(), FORMATTER);

                double distanceKm = Double.parseDouble(fields[5].trim());
                long durationSeconds =
                        java.time.Duration.between(pickup, dropoff).getSeconds();

                if (distanceKm <= 0 || durationSeconds <= 0) {
                    return;
                }

                double speedKmph = distanceKm / (durationSeconds / 3600.0);

                cityOut.set(city);
                speedOut.set(speedKmph);
                context.write(cityOut, speedOut);

            } catch (Exception ignored) {
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text city, Iterable<DoubleWritable> speeds,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            for (DoubleWritable speed : speeds) {
                sum += speed.get();
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
        Job job = Job.getInstance(conf, "Average Speed by City");

        job.setJarByClass(AvgSpeedByCity.class);
        job.setMapperClass(SpeedMapper.class);
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
