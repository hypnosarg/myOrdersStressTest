import com.google.gson.Gson;

import java.util.ArrayList;

public class TokensFile {
    public static TokensFile getInstance(String jsonData) {
        if (jsonData.charAt(0) == '[')
            //Remove starting and trailing brackets ([])
            jsonData = jsonData.substring(1, jsonData.length() - 1);
        return new Gson().fromJson(jsonData, TokensFile.class);
    }
    public class Token{
        public String access_token;
    }
    public ArrayList<Token> tokens;

}
