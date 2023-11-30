# bigdata-spark-sql


# Spark SQL
## Définition

Spark SQL est un module Apache Spark conçu pour le traitement et l'analyse de données structurées à l'aide du langage SQL. Il offre une interface unifiée permettant d'interagir avec des données structurées et semi-structurées, combinant les avantages du traitement distribué de Spark avec la familiarité du langage SQL pour l'analyse de données.

## Caractéristiques Principales

1. **Traitement Distribué :** Spark SQL s'intègre parfaitement avec le modèle de traitement distribué de Spark, facilitant la gestion efficace de grands ensembles de données répartis sur un cluster.

2. **Langage SQL :** Les utilisateurs peuvent interagir avec Spark SQL en utilisant des requêtes SQL standards, simplifiant l'adoption par ceux qui sont familiers avec le langage SQL.

3. **Support pour les Formats de Données :** Spark SQL prend en charge divers formats tels que Parquet, Avro, JSON, CSV, etc., permettant aux utilisateurs de travailler avec différents types de données sans conversion complexe.

4. **Intégration avec Hive :** Spark SQL s'intègre nativement avec Apache Hive, offrant un accès aux métadonnées Hive, aux tables et aux opérations HiveQL, assurant une compatibilité avec les systèmes existants basés sur Hive.

5. **Optimisations Catalyst :** Le moteur Catalyst est utilisé pour effectuer des optimisations avancées des requêtes, améliorant les performances du traitement en prédisant les prédicats, en projetant des colonnes, etc.

6. **APIs DataFrame et Dataset :** En plus des requêtes SQL, Spark SQL offre des APIs basées sur les concepts de DataFrame et de Dataset, permettant une expression programmatique des transformations de données dans divers langages.

## Cas d'utilisation

1. **Analyse de Données Structurées :** Spark SQL est idéal pour l'analyse de grands ensembles de données structurées, permettant aux utilisateurs de tirer parti du langage SQL pour obtenir des insights.

2. **Migration depuis Hive :** Les organisations utilisant Hive peuvent migrer leurs requêtes et leurs données vers Spark SQL tout en continuant à utiliser leurs scripts HiveQL existants.

3. **Intégration avec des Outils BI :** Grâce à son support SQL standard, Spark SQL peut être intégré avec des outils de business intelligence (BI) tels que Tableau, Power BI, etc., pour des analyses interactives.

# Dépendances Spark dans le fichier pom.xml (Maven)

Ajouter cette dépendance dans votre fichier pom.xml
```
     <dependency>
         <groupId>org.apache.spark</groupId>
         <artifactId>spark-core_2.13</artifactId>
         <version>3.4.1</version>
     </dependency>
     <dependency>
         <groupId>org.apache.spark</groupId>
         <artifactId>spark-sql_2.13</artifactId>
         <version>3.4.1</version>
     </dependency>
```
# Applications

## Analyser les incidents 
Nous avons l'intention de créer une application Spark destinée à une entreprise industrielle chargée du traitement des incidents de chaque service.

Les données relatives aux incidents sont stockées dans un fichier au format CSV.

![Description de l'image](/sparkSql/fichierCsv.PNG)

- La classe IncidentsAnalysis:

```java
package ma.enset;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class IncidentsAnalysis {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("Incidents Analysis").master("local[*]").getOrCreate();

       //Dataset<Row> csvData = spark.read().option("header", true).option("inferSchema", true).csv("incidents_services_2023.csv");

        String file = "incidents_services_2023.csv";
        Dataset<Row> dataset = spark.read().csv(file).toDF("id", "titre", "description","service" ,"date" );
        dataset.printSchema();
        Dataset<Row> incidentsPerService = dataset.groupBy(col("service"))
                .agg(count("id").alias("number_of_incidents"))
                .orderBy(desc("number_of_incidents"));

        incidentsPerService.show();

        //dataset.select("date").show();
        Dataset<Row> datasetWithDate = dataset.withColumn("date", to_date(col("date"), "yyyy-MM-dd"));
        Dataset<Row> incidentsYear = datasetWithDate.withColumn("year", year(col("date")));
        Dataset<Row> incidentsPerYear = incidentsYear.groupBy("year").count().orderBy(col("count").desc());

        incidentsPerYear.show(2);
        spark.close();

    }
}
```
- Afficher le nombre d’incidents par service:
  
![Description de l'image](/sparkSql/incidentsCount.PNG)

## Consultation Medicale

Créez une base de données MySQL nommée DB_HOPITAL, qui contient trois tables :
1. **PATIENTS**

![Description de l'image](/sparkSql/Patients.PNG)
   
2. **MEDECINS**

![Description de l'image](/sparkSql/Medecin.PNG)

3. **CONSULTATIONS**

![Description de l'image](/sparkSql/Consultations.PNG)

***Obtenir l'accès aux données de la base de données***
```java
        SparkSession ss = SparkSession.builder().appName("test mysql").master("local[*]").getOrCreate();
        Map<String , String > options = new HashMap< >( ) ;
        options.put( "driver" ,  "com.mysql.cj.jdbc.Driver" );
        options.put( "url" , "jdbc:mysql://localhost:3306/db_hopital" ) ;
        options.put( "user" , "root") ;
        options.put( "password" , "" ) ;

        Dataset<Row>  dataset = ss.read().format("jdbc")
                .options(options)
                .option("query" , "select * from MEDECINS ")
                .load();
        dataset.show();
```
1. ***Afficher le nombre de consultations par jour***
```java
 Dataset<Row>  dataset1 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT date_consultation, COUNT(*) as nombre_de_consultations FROM CONSULTATIONS GROUP BY date_consultation ORDER BY date_consultation")
                .load();
        dataset1.show();
```

![Description de l'image](/sparkSql/consParJour.PNG)
   
2. ***Afficher le nombre de consultation par médecin. Le format d’affichage est le suivant***

```NOM | PRENOM | NOMBRE DE CONSULTATION```

```java
  Dataset<Row>  dataset3 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT M.NOM, M.PRENOM, COUNT(C.ID_MEDECIN) AS NOMBRE_DE_CONSULTATION\n" +
                        "FROM MEDECINS M\n" +
                        "JOIN CONSULTATIONS C ON M.ID = C.ID_MEDECIN\n" +
                        "GROUP BY M.ID\n" +
                        "ORDER BY NOMBRE_DE_CONSULTATION DESC\n")
                .load();
        dataset3.show();
```

![Description de l'image](/sparkSql/ConsPar Med.PNG)

3. ***Afficher pour chaque médecin, le nombre de patients qu’il a assisté***

```java
  Dataset<Row>  dataset4 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT M.NOM, M.PRENOM, COUNT(DISTINCT C.ID_PATIENT) AS NOMBRE_DE_PATIENTS\n" +
                        "FROM MEDECINS M\n" +
                        "JOIN CONSULTATIONS C ON M.ID = C.ID_MEDECIN\n" +
                        "GROUP BY M.ID\n")
                .load();
        dataset4.show();
```

![Description de l'image](/sparkSql/NbrPatient.PNG)

# Conclusion

Spark SQL offre une solution puissante et polyvalente pour le traitement et l'analyse de données structurées dans des environnements distribués. Son intégration transparente avec le reste de l'écosystème Apache Spark, son support SQL, et ses fonctionnalités avancées en font un choix populaire pour les applications nécessitant une manipulation efficace de gros volumes de données structurées.
