package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank; // PageRank

import java.util.ArrayList;
import java.util.List;

import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.desc;

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		String dir = System.getProperty("hadoop.home.dir");

		GraphFrame gf;
		java.util.List<Row> vertices_list = new ArrayList<Row>();
		java.util.List<Row> edges_list = new ArrayList<Row>();

		// LOAD THE GRAPH
		try {
			File wikiVertices = new File(dir + "\\wiki-vertices.txt");
			File wikiEdges = new File(dir + "\\wiki-edges.txt");

			Scanner verticesReader = new Scanner(wikiVertices);
			Scanner edgesReader = new Scanner(wikiEdges);

			// Vertex DF reader
			while (verticesReader.hasNextLine()) {
				// data[0]: article ID -- data[1]: article title
				String[] data = verticesReader.nextLine().split("\t", 2);
				vertices_list.add(RowFactory.create(data[0], data[1]));
			}

			// Edge DF reader
			while (edgesReader.hasNextLine()) {
				// data[0]: source vertex -- data[1]: destination vertex
				String[] data = edgesReader.nextLine().split("\t", 2);
				edges_list.add(RowFactory.create(data[0], data[1]));
			}

		} catch (FileNotFoundException e) {
			System.out.println("An error occurred while reading the data");
			e.printStackTrace();
		}

		// Vertex DF creator
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		// Edge DF creator
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		// ENTITY LINK ANALYSIS

		gf = GraphFrame.apply(vertices,edges);
		gf.inDegrees().orderBy(desc("inDegree")).show(10);

		List<Double> resetProbList = Stream.of(0.15, 0.30, 0.45).collect(Collectors.toList());
		List<Integer> maxItersList = Stream.of(5, 10, 15).collect(Collectors.toList());

		// Grid Search
		for (Double resetP : resetProbList) { // damping factor (damping factor = 1 - resetP)
			for (Integer maxIt : maxItersList) { // maximum number of iterations
				long start = System.currentTimeMillis();
				System.out.println("---- PAGERANK ----");
				System.out.println("Damping factor: " + (1-resetP));
				System.out.println("Max Iterations: " + maxIt);
				System.out.println("----------------");
				PageRank pageRank = gf.pageRank().resetProbability(resetP).maxIter(maxIt);
				pageRank.run().vertices().select("id", "pagerank").orderBy(desc("pagerank")).show(10); // show top 10 most relevant articles
				long end = System.currentTimeMillis();
				System.out.println("Time (ms): " + (end - start));
				System.out.println("----------------");

			}
		}






	}
	
}
