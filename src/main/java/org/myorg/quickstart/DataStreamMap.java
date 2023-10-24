package org.myorg.quickstart;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class DataStreamMap extends ScalarFunction {

    public Row eval(Object... values) {
        //Object... values
        //return Row.of(a, "pre-" + a);
        // Row.of(row.getField(0))
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            //if (row.getField))
            row.setField(i, values[i]);
        }
        return row;


            /*int len = from.length;
            if (from.getArity() != len) {
                throw new RuntimeException("Row arity of from does not match serializers.");
            }
            Row result = new Row(len);
            for (int i = 0; i < len; i++) {
                Object fromField = from.getField(i);
                if (fromField != null) {
                    Object copy = fieldSerializers[i].copy(fromField);
                    result.setField(i, copy);
                } else {
                    result.setField(i, null);
                }
            }
            return result;*/

    }


    @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.ROW(Types.INT, Types.LOCAL_DATE, Types.STRING, Types.STRING, Types.STRING, Types.LOCAL_DATE);

    }


}
