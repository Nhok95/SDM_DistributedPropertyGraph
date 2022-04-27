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

class vertexValue implements Serializable{
    private Integer value;
    private List<Long> path;

    vertexValue(Integer value) {
        this.value = value;
        this.path = new ArrayList<>();
    }

    vertexValue(Integer value, ArrayList<Long> path) {
        this.value = value;
        this.path = path;
    }

    public Integer getValue() {
        return value;
    }

    public List<Long> getPath() {
        return path;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public void setPath(List<Long> path) {
        this.path = path;
    }

    public void addElement2Path(Long element) {
        this.path.add(element);
    }

    @Override
    public String toString() {
        return "vertexValue{" +
                "value=" + value +
                ", path=" + path +
                '}';
    }
}

public class Exercise_3 {

    // APPLY (vertex program):
    private static class vertexProgram extends AbstractFunction3<Long,vertexValue,vertexValue,vertexValue> implements Serializable {
        @Override
        public vertexValue apply(Long vertexID, vertexValue vertexValue, vertexValue message) {
            // System.out.println("--Vertex program for vertex:" + vertexID + "--\nMessage: " + message +"\nValue: " + vertexValue);
            // return the shortest past until the moment.
            if (vertexValue.getValue() > message.getValue()) { // message value has a shorter path value
                return message; // if the coming message has a lower value the message becomes the new vertex
            } else {
                return vertexValue;
            }

        }
    }

    // SCATTER (send message):
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<vertexValue,Integer>, Iterator<Tuple2<Object,vertexValue>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, vertexValue>> apply(EdgeTriplet<vertexValue, Integer> triplet) {
            Tuple2<Object,vertexValue> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,vertexValue> dstVertex = triplet.toTuple()._2();
            Integer edgeValue = triplet.toTuple()._3();


            if (sourceVertex._2.getValue() >= dstVertex._2.getValue() - edgeValue) {   // edge value is greater than dst vertex?
                // do nothing
                // System.out.println("-----sendMsg-----\nsourceVertex: " + sourceVertex + "\ndstVertex: " + dstVertex +"\nEdgeValue: " + edgeValue + "\nNothing");
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,vertexValue>>().iterator()).asScala();
            } else {
                // propagate source vertex value
                Integer newValue = sourceVertex._2.getValue() + edgeValue;
                ArrayList<Long> path = (ArrayList) sourceVertex._2.getPath();
                path.add((Long) dstVertex._1);

                vertexValue message = new vertexValue(newValue, path);

                // System.out.println("-----sendMsg-----\nsourceVertex: " + sourceVertex + "\ndstVertex: " + dstVertex +"\nEdgeValue: " + edgeValue + "\nPropagateEdge ("+ message +")");
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,vertexValue>(triplet.dstId(),message)).iterator()).asScala();
            }
        }
    }

    // GATHER (merge):
    private static class merge extends AbstractFunction2<vertexValue,vertexValue,vertexValue> implements Serializable {
        @Override
        public vertexValue apply(vertexValue o, vertexValue o2) {
            // Same as the vertexProgram
            if (o.getValue() > o2.getValue()) {
                return o2;
            } else {
                return o;
            }

            // return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        System.out.println("---- Exercise 3 ----");
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1L, "A")
                .put(2L, "B")
                .put(3L, "C")
                .put(4L, "D")
                .put(5L, "E")
                .put(6L, "F")
                .build();

        ArrayList<Long> path = new ArrayList<>();
        path.add(1l);
        List<Tuple2<Object,vertexValue>> vertices = Lists.newArrayList(
                new Tuple2<Object,vertexValue>(1L,new vertexValue(0, path)),
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

        Graph<vertexValue,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new vertexValue(0), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(vertexValue.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(vertexValue.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        ops.pregel(new vertexValue(Integer.MAX_VALUE),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new Exercise_3.vertexProgram(),
                new Exercise_3.sendMsg(),
                new Exercise_3.merge(),
                ClassTag$.MODULE$.apply(vertexValue.class))
            .vertices()
            .toJavaRDD().sortBy(f -> ((Tuple2<Object, Integer>) f)._1, true, 0)
            .foreach(v -> {
                Tuple2<Object,vertexValue> vertex = (Tuple2<Object,vertexValue>)v;
                List<String> pathLabels = new ArrayList<>();
                for (Long id : vertex._2.getPath()) {
                    pathLabels.add(labels.get(id));
                }

                System.out.println("Minimum cost to get from "+labels.get(1L)+" to "+labels.get(vertex._1)+" is "+ pathLabels + " with cost " + vertex._2.getValue() );
            });

        System.out.println("----------------");
    }
	
}
