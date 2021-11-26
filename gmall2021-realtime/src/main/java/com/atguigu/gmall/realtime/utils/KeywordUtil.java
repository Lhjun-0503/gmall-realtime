package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyword(String keyword) {

        //创建集合用于存放切分后的单词
        ArrayList<String> words = new ArrayList<>();

        StringReader stringReader = new StringReader(keyword);

        //创建IK分词对象  ik_smart  ik_max_word
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);

        //取出一个一个的单词

        try {
            Lexeme next = ikSegmenter.next();

            while (next != null) {
                String word = next.getLexemeText();

                //将切分好的单词存放入集合
                words.add(word);

                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return words;
    }

    public static void main(String[] args) {

        System.out.println(splitKeyword("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待"));

    }
}
