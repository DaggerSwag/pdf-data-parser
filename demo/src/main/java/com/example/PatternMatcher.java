package com.example;

import java.util.regex.*;

public class PatternMatcher {
    public boolean getMatchedString(String received_txt)
    {

        //regular exression(regex)
        String pattern="(\\d+)\\s[0-9]{2}-[A-Z]{3}-[23]{2}\\s[0-9]{2}:[0-9]{2}:[0-9]{2}\\s[0-9]{2}-[A-Z]{3}-[23]{2}\\s[0-9]{2}:[0-9]{2}:[0-9]{2}\\sJIONET\\s(\\d+)*(\\.\\d+)\\s(\\d+)*(\\.\\d+)\\s(\\d+)*(\\.\\d+)\\s(\\d+)*(\\.\\d+)\\s(\\d+)*(\\.\\d+)";
        boolean flag;
        flag = Pattern.compile(pattern).matcher(received_txt).matches();
        // if(flag)
        // {
        //     //trying to convert to csv format
        //     // System.out.println(received_txt.replace(' ', ','));
        //     // output_str=received_txt.replace(' ', ',');
        // }
        return flag;
    }
}
