package org.apache.giraph.examples;

import java.io.IOException;
import java.util.*;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class CommonFriendsComputation extends BasicComputation<LongWritable, 
CommonFriendsComputation.LongArrayListWritable, IntWritable, CommonFriendsComputation.LongArrayListWritable> {

  @Override
  public void compute(Vertex<LongWritable, LongArrayListWritable, IntWritable> vertex,
      Iterable<LongArrayListWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      // send list of this vertex's neighbors to all neighbors
      LongArrayListWritable outputList = new LongArrayListWritable();
      outputList.add(vertex.getId());
      for (Edge<LongWritable, IntWritable> edge : vertex.getEdges()) {
        outputList.add(new LongWritable(edge.getTargetVertexId().get()));
      }
      for (Edge<LongWritable, IntWritable> edge : vertex.getEdges()) {
        System.out.println(edge.getTargetVertexId() + " msg:" + outputList);
        sendMessage(edge.getTargetVertexId(), outputList);
      }
      
    } else if (getSuperstep() == 1){
      LongArrayListWritable vertextValue = vertex.getValue();
      if (vertextValue == null) {
        vertextValue = new LongArrayListWritable();
      }
      LongArrayListWritable fromList = new LongArrayListWritable();
      for (Edge<LongWritable, IntWritable> edge : vertex.getEdges()) {
        fromList.add(new LongWritable(edge.getTargetVertexId().get()));
      }
      for (LongArrayListWritable toList : messages) {
        LongWritable fromId = toList.get(0);
        System.out.println(vertex.getId() + " from " + fromList + " to "+ toList);
        long count = 0;
        vertextValue.add(fromId);
        int fromLen = fromList.size();
        int toLen = toList.size();
        int i = 0;
        int j = 1;
        while(i< fromLen && j < toLen) {
          if (fromList.get(i).get() == toList.get(j).get()) {
            count++;
            i++;
            j++;
          } else if (fromList.get(i).get() > toList.get(j).get()){
            j++;
          } else {
            i++;
          }
        }
        vertextValue.add(new LongWritable(count));
      }
      vertex.setValue(vertextValue);
    }
    
    if (getSuperstep() == 2){
      vertex.voteToHalt();
    }
  }
  
  public static class LongArrayListWritable extends ArrayListWritable<LongWritable> {
    /** Default constructor for reflection */
    public LongArrayListWritable() {
      super();
    }
    /** Set storage type for this ArrayListWritable */
    @Override
    @SuppressWarnings("unchecked")
    public void setClass() {
      setClass(LongWritable.class);
    }
  }
}
