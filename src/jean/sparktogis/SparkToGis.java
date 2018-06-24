package jean.sparktogis;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;



/**
 * Classe que obtem os dados de arquivo CSV e inseri na base de dados.
 * @author jean
 *
 */
public class SparkToGis {

	private static String URI_BASE_DADOS =  "jdbc:postgres://localhost:5432/Aula01?currentSchema-Trabalho01";

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession
				.builder()
				.appName("My Spark To QGis")
				.master("local[*]").getOrCreate();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "postgres");
		connectionProperties.put("password", "jean");

		Dataset<Row> ds_branco = loadCsv(sparkSession, "/home/jean/Documentos/Trabalho01/COR_DA_PELE_BRASIL/Brancos.csv");
		ds_branco.write().jdbc(URI_BASE_DADOS,"branco", connectionProperties);

		Dataset<Row> ds_amarelo = loadCsv(sparkSession, "/home/jean/Documentos/Trabalho01/COR_DA_PELE_BRASIL/Amarelos.csv");
		ds_amarelo.write().jdbc(URI_BASE_DADOS,"amarelo", connectionProperties);

		Dataset<Row> ds_indio = loadCsv(sparkSession, "/home/jean/Documentos/Trabalho01/COR_DA_PELE_BRASIL/Indigenas.csv");
		ds_indio.write().jdbc(URI_BASE_DADOS,"indio", connectionProperties);

		Dataset<Row> ds_pardo = loadCsv(sparkSession, "/home/jean/Documentos/Trabalho01/COR_DA_PELE_BRASIL/Pardos.csv");
		ds_pardo.write().jdbc(URI_BASE_DADOS,"pardo", connectionProperties);

		Dataset<Row> ds_preto = loadCsv(sparkSession, "/home/jean/Documentos/Trabalho01/COR_DA_PELE_BRASIL/Pretos.csv");
		ds_preto.write().jdbc(URI_BASE_DADOS,"preto", connectionProperties);

		sparkSession.cloneSession();
	}

	public static Dataset<Row> loadCsv(SparkSession session, String path_file) {

		Dataset<Row> load_csv = session.read()
				.format("com.databricks.spark.csv")
				.option("header", true)
				.option("delimiter", ";")
				.option("inferSchema", true)
				.load(path_file);

		Dataset<Row> ds = load_csv.select("LAT","LON","_c2").withColumnRenamed("_c2", "geo");
		
		return ds;

	}

}