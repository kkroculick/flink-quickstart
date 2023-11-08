package org.myorg.quickstart;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class DataStreamFlatMap  extends TableFunction<Row> {

 /*   public void eval(String str) {
        if (str.contains("#")) {
            String[] array = str.split("#");
            for (int i = 0; i < array.length; ++i) {
                collect(Row.of(array[i], array[i].length()));
            }
        }
    }*/

    public void eval(Object... values) {

       Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {

            row.setField(i, values[i]);
            //Row.of(i, values[i])
        }
        collect(row);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.INT, Types.LOCAL_DATE, Types.STRING, Types.STRING, Types.STRING, Types.LOCAL_DATE);

    }
}
