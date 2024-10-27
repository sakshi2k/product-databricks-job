package skudata;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

import static org.apache.spark.sql.functions.udf;

@Slf4j
public class ValueUdfs {
    
    private final UserDefinedFunction checkDistanceForValues = udf((UDF2<String, Seq<String>, String>) (productDataValue, values) -> {
        log.info("Inside checkDistanceForValues()");
        try {
            List<String> valueList = JavaConverters.seqAsJavaList(values);
            Map<String, Double> list = new HashMap<>();
            for (String value : valueList) {
                list.put(value, Double.valueOf(2d));  // Assuming a fixed distance for the example
            }
            log.info("list in checkDistanceForValues : {}", list);
            if (list.isEmpty()) {
                log.info("list in checkDistanceForValues is empty");
                return null;
            }
            double min = Double.MAX_VALUE;
            String closestValue = null;
            for (Map.Entry<String, Double> entry : list.entrySet()) {
                if (min > entry.getValue()) {
                    min = entry.getValue();
                    closestValue = entry.getKey();
                }
            }
            return closestValue;
        } catch (Exception e) {
            log.error("Error in checkDistanceForValues UDF", e);
            return null;
        }
    }, DataTypes.StringType);
    
    public Column getValuesUdf(Column value, List<Column> valueNameList) {
        log.info("inside getValuesUdf() with valueNameList : {}", valueNameList);
        Column res = this.checkDistanceForValues.apply(value, functions.array(valueNameList.toArray(new Column[0])));
        log.info("exiting getValue() with res : {}", res);
        return res;
    }

}