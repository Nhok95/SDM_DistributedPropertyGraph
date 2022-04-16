package exercise_2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_2 {

    // APPLY (vertex program):
    private static class vertexProgram extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            return null;
        }
    }

    // SCATTER (send message):
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            return null;
        }
    }

    // GATHER (merge):
    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return null;
        }
    }

	public static void shortestPaths(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1L, "A")
                .put(2L, "B")
                .put(3L, "C")
                .put(4L, "D")
                .put(5L, "E")
                .put(6L, "F")
                .build();

        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
                new Tuple2<Object,Integer>(1L,0),
                new Tuple2<Object,Integer>(2L,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(3L,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(4L,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(5L,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(6L,Integer.MAX_VALUE)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1L,2L, 4), // A --> B (4)
                new Edge<Integer>(1L,3L, 2), // A --> C (2)
                new Edge<Integer>(2L,3L, 5), // B --> C (5)
                new Edge<Integer>(2L,4L, 10), // B --> D (10)
                new Edge<Integer>(3L,5L, 3), // C --> E (3)
                new Edge<Integer>(5L, 4L, 4), // E --> D (4)
                new Edge<Integer>(4L, 6L, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new vertexProgram(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Integer.class))
            .vertices()
            .toJavaRDD()
            .foreach(v -> {
                Tuple2<Object,Integer> vertex = (Tuple2<Object,Integer>)v;
                System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+vertex._2);
            });
	}
	
}
