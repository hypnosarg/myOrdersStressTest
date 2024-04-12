import com.google.gson.Gson;

import java.util.ArrayList;

public class ConfigsFile {
    public static ConfigsFile getInstance(String jsonData) {
        if (jsonData.charAt(0) == '[')
            //Remove starting and trailing brackets ([])
            jsonData = jsonData.substring(1, jsonData.length() - 1);
        return new Gson().fromJson(jsonData, ConfigsFile.class);
    }
    public int order_chance;
    public int stock_chance;
    public int minrayon_chance;
    public int inassort_chance;
    public int inrao_chance;
    public int max_syncs;
    public int sync_interval_mins;
    public int seconds_sync_check;

}
