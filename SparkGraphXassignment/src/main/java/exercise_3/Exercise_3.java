package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import exercise_2.Exercise_2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
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

class vertexValue {
    private Integer value = 0;
    private ArrayList<Long> path = new ArrayList<Long>();

    vertexValue() {
        this.value = value;
    }

    vertexValue(Integer value) {
        this.value = value;
    }

    vertexValue(Integer value, ArrayList<Long> path) {
        this.value = value;
        this.path = path;
    }

    public Integer getValue() {
        return value;
    }

    public ArrayList<Long> getPath() {
        return path;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public void setPath(ArrayList<Long> path) {
        this.path = path;
    }

    public void addElement2Path(Long element) {
        this.path.add(element);
    }
}

public class Exercise_3 {

    // APPLY (vertex program):
    private static class vertexProgram extends AbstractFunction3<Long,vertexValue,vertexValue,vertexValue> implements Serializable {
        @Override
        public vertexValue apply(Long vertexID, vertexValue vertexValue, vertexValue message) {
            //System.out.println("--Vertex program for vertex:" + vertexID + "--\nMessage: " + message +"\nValue: " + vertexValue + "\nMin: " + Math.min(vertexValue,message));
            // return the shortest past until the moment.

            // new vertex value has by default the original vertex value.
            Integer newMin = vertexValue.getValue();
            ArrayList<Long> newPath = vertexValue.getPath();

            // if the coming message has a lower value the message becomes the new vertex
            if (vertexValue.getValue() > message.getValue()) { // message value has a shorter path value
                newMin = message.getValue();
                newPath = message.getPath();
            }

            // Finally, we add the current vertexID to the Path
            newPath.add(vertexID);

            return new vertexValue(newMin, newPath); //new value for the vertex vertexID

        }
    }

    // SCATTER (send message):
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<vertexValue,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<vertexValue, Integer> triplet) {
            Tuple2<Object,vertexValue> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,vertexValue> dstVertex = triplet.toTuple()._2();
            Integer edgeValue = triplet.toTuple()._3();


            if (sourceVertex._2.getValue() >= dstVertex._2.getValue() - edgeValue) {   // edge value is greater than dst vertex?
                // do nothing
                System.out.println("-----sendMsg-----\nsourceVertex: " + sourceVertex + "\ndstVertex: " + dstVertex +"\nEdgeValue: " + edgeValue + "\nNothing");
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                System.out.println("-----sendMsg-----\nsourceVertex: " + sourceVertex + "\ndstVertex: " + dstVertex +"\nEdgeValue: " + edgeValue + "\nPropagateSource");
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2.getValue()+edgeValue)).iterator()).asScala();
            }
        }
    }

    // GATHER (merge):
    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {

            return null;//Math.min(o, o2); // return the shortest path between 2 options
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {

        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1L, "A")
                .put(2L, "B")
                .put(3L, "C")
                .put(4L, "D")
                .put(5L, "E")
                .put(6L, "F")
                .build();

        List<Tuple2<Object,vertexValue>> vertices = Lists.newArrayList(
                new Tuple2<Object,vertexValue>(1L,new vertexValue()),
                new Tuple2<Object,vertexValue>(2L,new vertexValue(Integer.MAX_VALUE)),
                new Tuple2<Object,vertexValue>(3L,new vertexValue(Integer.MAX_VALUE)),
                new Tuple2<Object,vertexValue>(4L,new vertexValue(Integer.MAX_VALUE)),
                new Tuple2<Object,vertexValue>(5L,new vertexValue(Integer.MAX_VALUE)),
                new Tuple2<Object,vertexValue>(6L,new vertexValue(Integer.MAX_VALUE))
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

        JavaRDD<Tuple2<Object,vertexValue>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<vertexValue,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new vertexValue(), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(vertexValue.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(vertexValue.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        ops.pregel(new vertexValue(Integer.MAX_VALUE),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.vertexProgram(),
                new Exercise_3.sendMsg(),
                new Exercise_3.merge(),
                ClassTag$.MODULE$.apply(Integer.class))
            .vertices()
            .toJavaRDD()
            .foreach(v -> {
                Tuple2<Object,vertexValue> vertex = (Tuple2<Object,vertexValue>)v;
                System.out.println("Minimum cost to get from "+labels.get(1L)+" to "+labels.get(vertex._1)+" is "+ vertex._2.getPath() + "with cost " + vertex._2.getValue() );
            });

    }
	
}
