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

import java.util.ArrayList;
import java.util.List;

import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

public class Exercise_4 {

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		String dir = System.getProperty("hadoop.home.dir");

		// LOAD THE GRAPH
		try {
			File wikiVertices = new File(dir + "\\wiki-vertices.txt");
			File wikiEdges = new File(dir + "\\wiki-edges.txt");

			Scanner verticesReader = new Scanner(wikiVertices);
			Scanner edgesReader = new Scanner(wikiEdges);

			// Vertex DF creation
			java.util.List<Row> vertices_list = new ArrayList<Row>();
			while (verticesReader.hasNextLine()) {
				// data[0]: article ID -- data[1]: article title
				String[] data = verticesReader.nextLine().split("\t", 2);
				vertices_list.add(RowFactory.create(data[0], data[1]));
			}

			JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

			StructType vertices_schema = new StructType(new StructField[]{
					new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build()),
			});

			// Edge DF creation
			java.util.List<Row> edges_list = new ArrayList<Row>();
			while (verticesReader.hasNextLine()) {
				// data[0]: source vertex -- data[1]: destination vertex
				String[] data = verticesReader.nextLine().split("\t", 2);

			}

		} catch (FileNotFoundException e) {
			System.out.println("An error occurred while reading the data");
			e.printStackTrace();
		}




	}
	
}
