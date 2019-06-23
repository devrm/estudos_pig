package org.usp.hadoopdemo.function;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Optional;

public class PigStringFunctionDemo extends EvalFunc<String> {


    @Override
    public String exec(Tuple tuple) throws IOException {
        if (tuple != null && tuple.size() > 0) {
            Object tupleObj = tuple.get(0);
            if (tupleObj != null) {
                final String first = (String) tupleObj;

                String formatted = first.replaceAll("[^a-zA-Z0-9\\-]", "");
                return formatted;

            }
        }

        return "";
    }
}
