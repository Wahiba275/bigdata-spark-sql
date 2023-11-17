package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import java.util.Map;

public class MedicalConsultation {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("test mysql").master("local[*]").getOrCreate();
        Map<String , String > options = new HashMap< >( ) ;
        options.put( "driver" ,  "com.mysql.cj.jdbc.Driver" );
        options.put( "url" , "jdbc:mysql://localhost:3306/db_hopital" ) ;
        options.put( "user" , "root") ;
        options.put( "password" , "" ) ;

        /*Dataset<Row>  dataset = ss.read().format("jdbc")
                .options(options)
                .option("query" , "select * from MEDECINS ")
                .load();
        dataset.show();*/

        Dataset<Row>  dataset1 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT date_consultation, COUNT(*) as nombre_de_consultations FROM CONSULTATIONS GROUP BY date_consultation ORDER BY date_consultation")
                .load();
        dataset1.show();

        Dataset<Row>  dataset3 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT M.NOM, M.PRENOM, COUNT(C.ID_MEDECIN) AS NOMBRE_DE_CONSULTATION\n" +
                        "FROM MEDECINS M\n" +
                        "JOIN CONSULTATIONS C ON M.ID = C.ID_MEDECIN\n" +
                        "GROUP BY M.ID\n" +
                        "ORDER BY NOMBRE_DE_CONSULTATION DESC\n")
                .load();
        dataset3.show();

        Dataset<Row>  dataset4 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT M.NOM, M.PRENOM, COUNT(DISTINCT C.ID_PATIENT) AS NOMBRE_DE_PATIENTS\n" +
                        "FROM MEDECINS M\n" +
                        "JOIN CONSULTATIONS C ON M.ID = C.ID_MEDECIN\n" +
                        "GROUP BY M.ID\n")
                .load();
        dataset4.show();

    }
}
