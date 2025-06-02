package org.myorg.quickstart.processor;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class DataStreamMap extends ScalarFunction {

    public Row eval(Object... values) throws Exception {

        if (values == null) {
            return null;
        }
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;

    }

   /* public Row eval(Row record) {

          // int len = record.length;

            int len = record.getArity();
            Row result = new Row(len);
            for (int i = 0; i < len; i++) {
                Object fromField = record.getField(i);
                if (fromField != null) {
                    Object copy = record.getField((Integer) fromField);
                    result.setField(i, copy);
                } else {
                    //result.setField(i, null);
                }
            }
            return result;

    }*/


    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.ROW(Types.INT, Types.LOCAL_DATE, Types.STRING, Types.STRING, Types.STRING, Types.LOCAL_DATE);

    }


}
