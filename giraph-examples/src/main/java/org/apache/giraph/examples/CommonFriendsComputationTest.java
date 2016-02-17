package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.io.formats.IntIntNullTextInputFormat.IntIntNullVertexReader;
import org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReaderFromEachLineProcessed;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CommonFriendsComputationTest {

  public static void main(String[] args) throws Exception {

    String[] graph = {
        "5 1 2",
        "1 2 5",
        "2 5",
        "6 7",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(CommonFriendsComputation.class);
    conf.setVertexInputFormatClass(CommonFriendsTextInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results = InternalVertexRunner.run(conf, graph);

    Map<Integer, Integer> values = parseResults(results);
  }
  
  private static Map<Integer, Integer> parseResults(Iterable<String> results) {
    Map<Integer, Integer> values = new HashMap<Integer, Integer>();
    for (String line : results) {
      //String[] tokens = line.split("\\s+");
      //int id = Integer.valueOf(tokens[0]);
      //int value = Integer.valueOf(tokens[1]);
      //values.put(id, value);
      System.out.println("result =" + line);
    }
    return values;
  }
  
  public static class CommonFriendsTextInputFormat extends
      TextVertexInputFormat<LongWritable, CommonFriendsComputation.LongArrayListWritable, IntWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
        TaskAttemptContext context) throws IOException {
      return new CommonFriendsVertexReader();
    }

    /**
     * Vertex reader associated with {@link CommonFriendsTextInputFormat}.
     */
    public class CommonFriendsVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {
      /**
       * Cached vertex id for the current line
       */
      private LongWritable id;

      @Override
      protected String[] preprocessLine(Text line) throws IOException {
        String[] tokens = SEPARATOR.split(line.toString());
        id = new LongWritable(Integer.parseInt(tokens[0]));
        return tokens;
      }

      @Override
      protected LongWritable getId(String[] tokens) throws IOException {
        return id;
      }

      @Override
      protected CommonFriendsComputation.LongArrayListWritable getValue(
          String[] tokens) throws IOException {
        return new CommonFriendsComputation.LongArrayListWritable();
      }

      @Override
      protected Iterable<Edge<LongWritable, IntWritable>> getEdges(
          String[] tokens) throws IOException {
        List<Edge<LongWritable, IntWritable>> edges = 
            new ArrayList<Edge<LongWritable, IntWritable>>(tokens.length - 1);
        for (int n = 1; n < tokens.length; n++) {
          edges.add(EdgeFactory.create(
              new LongWritable(Integer.parseInt(tokens[n])),new IntWritable(0)));
        }
        return edges;
      }
    }
  }

}
