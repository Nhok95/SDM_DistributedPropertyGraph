package exercise_1;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD; // Java resilient distributed dataset (RDD)
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Exercise_1 {

    // APPLY (vertex program):
    // Applies a user-defined function `f` to each vertex in parallel; meaning that `f` specifies the behaviour of a single vertex v
    // at a particular superstep S. On the first iteration, the vertex program is invoked on all vertices and the pre-defined
    // message is passed. On subsequent iterations, the vertex program is only invoked on those vertices that receive messages.
    private static class vertexProgram extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            if (message == Integer.MAX_VALUE) {             // superstep 0
                System.out.println("--Vertex program for vertex:" + vertexID + "--\nValue(0): " + vertexValue +"\nMessage(0): " + message);
                return vertexValue;
            } else {                                        // superstep > 0
                System.out.println("--Vertex program for vertex:" + vertexID + "--\nMessage: " + message +"\nValue: " + vertexValue + "\nMax: " + Math.max(vertexValue,message));
                return Math.max(vertexValue,message);
            }
        }
    }

    // SCATTER (send message):
    // May send messages to other vertices, such that those vertices will receive the messages in the next superstep S + 1
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();

            if (sourceVertex._2 <= dstVertex._2) {   // source vertex value is smaller than dst vertex?
                // do nothing
                System.out.println("-----sendMsg-----\nsourceVertex: " + sourceVertex + "\ndstVertex: " + dstVertex + "\nNothing");
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                System.out.println("-----sendMsg-----\nsourceVertex: " + sourceVertex + "\ndstVertex: " + dstVertex + "\nPropagateSource");
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2)).iterator()).asScala();
            }
        }
    }

    // GATHER (merge):
    // Receives and reads messages that are sent to a node v from the previous superstep S-1.
    // This function must be commutative and associative.
    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {return null;}
    }

    public static void maxValue(JavaSparkContext ctx) {
        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
            new Tuple2<Object,Integer>(1L,9),
            new Tuple2<Object,Integer>(2L,1),
            new Tuple2<Object,Integer>(3L,6),
            new Tuple2<Object,Integer>(4L,8)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
            new Edge<Integer>(1L,2L, 1),
            new Edge<Integer>(2L,3L, 1),
            new Edge<Integer>(2L,4L, 1),
            new Edge<Integer>(3L,4L, 1),
            new Edge<Integer>(3L,1L, 1)
        );

        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // Call to the `Pregel` framework. The parameters are as follows:
        // * initialMsg: is the message that all vertices will receive at the start of superstep 0.
        //               Here we use Integer.MAX_VALUE just for the purpose of identifying supertep 0
        // * maxIter: indicates the maximum number of iterations (i.e. supersteps).
        //            Here we set it as Integer.MAX_VALUE as convergence is guaranteed.
        // * activeDir: refers to the edge direction in which to send the message.
        // * Apply function
        // * Scatter function
        // * Gather function
        // * evidence: the definition of the class being passed as message. Note the usage of
        //              scala.reflect.ClassTag$.MODULE$.apply(Integer.class), which uses Scala's API.
        // returns a tuple with <VertexID, Value>
        Tuple2<Long,Integer> max = (Tuple2<Long,Integer>)ops.pregel(
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,      // Run until convergence
                EdgeDirection.Out(),
                new vertexProgram(),
                new sendMsg(),
                new merge(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class))
        .vertices().toJavaRDD().first();
        
        System.out.println(max._2 + " is the maximum value in the graph");
	}
	
}
