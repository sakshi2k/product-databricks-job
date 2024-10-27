package skudata;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.*;

public class ProcessProductData {
    private SparkSession sparkSession;

    public void main(String[] args){
      ValueUdfs valueUdfs = new ValueUdfs();

      this.sparkSession = SparkSession.builder().appName("UDF for checking Values for sku product data").getOrCreate();

      List<String> values = getStaticValuesList();
      List<Column> valueNameList = getValueNameColumnsList(values);

      Dataset<Row> productData = getProductData();
      productData = productData.select(
              productData.col("COMP_SKU").alias("SKU"),
              valueUdfs.getValuesUdf(productData.col("BRAND_NAME"), valueNameList).alias("BRAND")
      );
      productData.show();
    }

    private static List<Column> getValueNameColumnsList(List<String> values) {
      List<Column> valueNames = new ArrayList<>();
      for (String value : values) {
        valueNames.add(functions.lit(value.trim().toLowerCase()));
      }
      return valueNames;
    }

    private Dataset<Row> getProductData() {
      List<Row> data = getStaticProductData();
      StructType schema = new StructType(new StructField[]{
              new StructField("COMP_SKU", DataTypes.StringType, false, Metadata.empty()),
              new StructField("BRAND_NAME", DataTypes.StringType, false, Metadata.empty())
      });
      return this.sparkSession.createDataFrame(data, schema);
    }

    private static List<String> getStaticValuesList() {
      return Arrays.asList("Value_a", "Value_b", "Value_c");
    }

    private static List<Row> getStaticProductData() {
      return Arrays.asList(
              RowFactory.create("SKU_1", "Value_a"),
              RowFactory.create("SKU_2", "Value_a"),
              RowFactory.create("SKU_3", "Value_a"),
              RowFactory.create("SKU_11", "Value_d"),
              RowFactory.create("SKU_22", "Value_d"),
              RowFactory.create("SKU_55", "Value_e")
      );
    }

}