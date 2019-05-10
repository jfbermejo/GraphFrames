/*
 * PRÁCTICA DE SISTEMAS DE INFORMACIÓN
 * Máster en Ingeniería Informática
 * Escuela Politécnica - UEX
 *
 * Uso de GraphFrames
 *
 * Bermejo Martín, Juan Francisco
 * Bravo Gómez, Alberto
 * Carrasco Santano, Irene
 * Vázquez Cordero, Cristian
 *
 */


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {

    public static void main(String[] args) {

        String csvAeropuertos = "data/aeropuertos_reducido.csv";
        String csvRutas = "data/rutas_reducido.csv";
        BufferedReader br = null;
        String line = "";

        List<Aeropuerto> aeropuertosList = new ArrayList<Aeropuerto>();
        List<Ruta> rutasList = new ArrayList<Ruta>();

        //Se define separador ","
        String cvsSplitBy = ",";

        try {

            br = new BufferedReader(new FileReader(csvAeropuertos));

            while ((line = br.readLine()) != null) {
                String[] datos = line.split(cvsSplitBy);

                Aeropuerto aeropuertoAux = new Aeropuerto(datos[0], datos[1], datos[2], datos[3],
                        Float.parseFloat(datos[4]), Float.parseFloat(datos[5]));

                aeropuertosList.add(aeropuertoAux);
            }

            System.out.println("Se cargaron datos de " + aeropuertosList.size() + " aeropuertos");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        try {

            br = new BufferedReader(new FileReader(csvRutas));

            while ((line = br.readLine()) != null) {
                String[] datos = line.split(cvsSplitBy);

                Ruta rutaAux = new Ruta(datos[0], datos[1], datos[2], Integer.parseInt(datos[3]));

                rutasList.add(rutaAux);
            }

            System.out.println("Se cargaron datos de " + rutasList.size() + " rutas");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



        // CREACIÓN DEL GRAFO

        SparkSession spark = SparkSession.builder()
                .appName("Main")
                .config("spark.sql.warehouse.dir", "/file:C:/temp")
                .master("local[2]")
                .getOrCreate();

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        Dataset<Row> verDF = spark.createDataFrame(aeropuertosList, Aeropuerto.class);

        Dataset<Row> edgDF = spark.createDataFrame(rutasList, Ruta.class);

        //Create a GraphFrame
        GraphFrame gFrame = new GraphFrame(verDF, edgDF);

        // Consultas

        System.out.println("Número total de aeropuertos en el grafo: " + gFrame.vertices().count() );
        System.out.println("Número total de rutas en el grafo: " + gFrame.edges().count() );

        System.out.println();

        System.out.println("==== Listado de aeropuertos: ====");
        gFrame.vertices().show();

        System.out.println("==== Listado de rutas: ====");
        gFrame.edges().show();

        System.out.println("==== La ruta de más longitud entre dos aeropuertos: ====");
        Dataset<Row> df = gFrame.edges().select("src", "dst", "type", "distance");
        df = df.orderBy(df.col("distance").asc());
        df.show(1);

        System.out.println("==== La ruta de menor longitud entre dos aeropuertos: ====");
        df = gFrame.edges().select("src", "dst", "type", "distance");
        df = df.orderBy(df.col("distance").desc());
        df.show(1);

        // El aeropuerto más al este de los registrados
        System.out.println("==== El aeropuerto más al ESTE, tiene una longitud de: ====");
        df = gFrame.vertices().select("id", "name", "city", "country", "lat", "lon");
        df = df.orderBy(df.col("lon").asc());
        df.show(1);

        // El aeropuerto más al oeste de los registrados
        System.out.println("==== El aeropuerto más al OESTE, tiene una longitud de: ====");
        df = gFrame.vertices().select("id", "name", "city", "country", "lat", "lon");
        df = df.orderBy(df.col("lon").desc());
        df.show(1);

        // El aeropuerto más cercano al ecuador
        System.out.println("==== Los aeropuertos más cercano al ecuador, tienen una latitud de: ====");
        Dataset<Row> auxDS = gFrame.vertices().filter("lat > 0.0");
        GraphFrame auxGraph = new GraphFrame(auxDS , edgDF);
        df = auxGraph.vertices().select("id", "name", "city", "country", "lat", "lon");
        df = df.orderBy(df.col("lat").asc());
        df.show(1);

        // El aeropuerto más cercano al meridiano de  Greenwich
        System.out.println("==== Los aeropuertos más cercanos al meridiano de  Greenwich, tienen una longitud de: ====");
        auxDS = gFrame.vertices().filter("lon < 0.0");
        auxGraph = new GraphFrame(auxDS,edgDF);
        df = auxGraph.vertices().select("id", "name", "city", "country", "lat", "lon");
        df = df.orderBy(df.col("lon").desc());
        df.show(1);

        // La ruta que une Nashville International Airport (BNA)
        // y Fort Lauderdale/Hollywood International Airport (FLL)
        System.out.println("==== Ruta entre Nashville International Airport (BNA)" +
                "y Fort Lauderdale/Hollywood International Airport (FLL) ====");
        gFrame.bfs().fromExpr("id = 'BNA'").toExpr("id = 'FLL'").run().show();

        // La ruta que une Dallas/Fort Worth International Airport (DFW)
        // y Anchorage Ted Stevens (ANC)
        System.out.println("==== Ruta entre Dallas/Fort Worth International Airport (DFW)" +
                "y Anchorage Ted Stevens (ANC) ====");
        gFrame.bfs().fromExpr("id = 'DFW'").toExpr("id = 'ANC'").run().show();

        // Muestra el grado de cada nodo
        System.out.println("==== Número de rutas que llegan a cada aeropuerto ====");
        gFrame.inDegrees().show();

        System.out.println("==== Número de rutas que salen de cada aeropuerto ====");
        gFrame.outDegrees().show();

        System.out.println("==== Número de rutas totales de cada aeropuerto ====");
        gFrame.degrees().show();

        // Algoritmo PageRank aplicado a Aeropuertos
        System.out.println("==== Algoritmo de pagerank de cada aeropuerto ====");
        GraphFrame results = gFrame.pageRank().resetProbability(0.15).maxIter(10).run();
        results.vertices().select("city", "country", "id", "pagerank").show();

        // Calcula las escalas de todos los areopuertos a los especificados en la lista
        ArrayList<Object> list = new ArrayList<Object>();
        list.add("DCA");
        list.add("IAD");
        Dataset<Row> results1 = gFrame.shortestPaths().landmarks(list).run();
        results1.select("name", "city", "distances","id" ).show( );

        // Muestra los aeropuertos que tienen rutas de ida y vuelta
        // directas definidos entre ellos.

        // Motif: A->B and B->A
        System.out.println("==== Aeropuertos con rutas de ida y vuelta directas: ====");
        Dataset<Row> motifsTwoBid = gFrame.find("(A)-[]->(B); (B)-[]->(A)");
        motifsTwoBid = motifsTwoBid.select("A","A.id", "B","B.id").distinct();
        motifsTwoBid.show();

        // Consulta que obtiene un aeropuerto intermedio entre dos
        // que no tienen ruta definida entre ellos.

        // Motif: A->B->C but not A->C
        System.out.println("==== Aeropuertos que conectan la ruta entre otros dos: ====");
        Dataset<Row> motifsTriplet = gFrame.find("(A)-[]->(B); (B)-[]->(C); !(A)-[]->(C)");
        motifsTriplet = motifsTriplet.filter("A.id != C.id");
        motifsTriplet = motifsTriplet.select("A","A.id", "B", "B.id", "C", "C.id");
        motifsTriplet.show();

        //stop
        spark.stop();


    }
}
