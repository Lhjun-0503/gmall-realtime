package realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtilTest01 {

    public static List<String> splitKeyword(String keyword) throws IOException {

        //创建集合存放切分后的单词
        ArrayList<String> result = new ArrayList<>();

        //创建分词器对象
        StringReader stringReader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,false);

        //获取下一个词元对象
        Lexeme next = ikSegmenter.next();

        while(next != null) {

            String lexemeText = next.getLexemeText();

            result.add(lexemeText);

            next = ikSegmenter.next();
        }

        return result;
    }
}
