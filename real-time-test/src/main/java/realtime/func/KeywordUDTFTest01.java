package realtime.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import realtime.utils.KeywordUtilTest01;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("Row<word STRING>"))
public class KeywordUDTFTest01 extends TableFunction {

    public void eval(String keyword) {


        try {
            //切词
            List<String> list = KeywordUtilTest01.splitKeyword(keyword);

            for (String word : list) {

                collect(Row.of(word));
            }

        } catch (IOException e) {
            collect(keyword);
        }
    }
}
