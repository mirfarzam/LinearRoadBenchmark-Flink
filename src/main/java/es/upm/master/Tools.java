package es.upm.master;

import java.util.HashMap;

public class Tools {

    static HashMap<String, String> convertToKeyValuePair(String[] args) {
        HashMap<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i=i+2) {
            params.put(args[i].substring(1), args[i+1]);
        }
        return params;
    }


}
