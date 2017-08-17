package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		// input: key=node_identifier, value=Iterable<Text> values
		// emit: key=node_identifier+(rank_value), value= adjacency list
		StringBuilder sb = new StringBuilder();
		for (Text val : values){
			sb.append(val.toString() + " ");
		}
		context.write(new Text(key.toString() + PageRankDriver.MARKER_DELIMITER + "1.0"), new Text(sb.toString().trim()));
	}
}
