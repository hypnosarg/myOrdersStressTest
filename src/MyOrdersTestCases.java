import com.google.gson.Gson;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Random;

public class MyOrdersTestCases {
    private static final String FLAG_CHECKED = "X";

    public class TestData {
        public String HOPE;
        public String ORDERABLEDATE;
        public String EXISTINGQTY;
        public String QTYSETLMTSTPM;
        public String ORDERSESSION;
        public String USED;
    }

    public class TestHeader {
        public String TESTCODE;
        public String TESTDESCRIPTION;
        public ArrayList<TestData> SCENARIOS;
    }

    public String STORE;
    public String WERKS;
    public ArrayList<TestHeader> TESTCASES;

    public static MyOrdersTestCases getInstance(String jsonData) {
        if (jsonData.charAt(0) == '[')
            //Remove starting and trailing brackets ([])
            jsonData = jsonData.substring(1, jsonData.length() - 1);
        return new Gson().fromJson(jsonData, MyOrdersTestCases.class);
    }

    public String getJson() {
        return new Gson().toJson(this);
    }


    public ArrayList<TestData> getArticlesForUpdate(Boolean flagAsUsed, int qty, boolean randomOrder) {
        return getCasesForScenario("10", flagAsUsed, qty, randomOrder);
    }

    public ArrayList<TestData> getArticlesForCreation(Boolean flagAsUsed, int qty, boolean randomOrder) {
        return getCasesForScenario("9", flagAsUsed, qty, randomOrder);
    }
    public ArrayList<TestData> getArticlesForStock(Boolean flagAsUsed, int qty, boolean randomOrder) {
        return getCasesForScenario("14", flagAsUsed, qty, randomOrder);
    }
    public ArrayList<TestData> getArticlesForMinRayon(Boolean flagAsUsed, int qty, boolean randomOrder) {
        return getCasesForScenario("13", flagAsUsed, qty, randomOrder);
    }

    public ArrayList<TestData> getArticlesForInAssort(Boolean flagAsUsed, int qty, boolean randomOrder) {
        return getCasesForScenario("15", flagAsUsed, qty, randomOrder);
    }


    public ArrayList<TestData> getCasesForScenario(String scenario, Boolean flagAsUsed, int qty, Boolean randomOrder) {
        ArrayList<TestData> cases = new ArrayList<>();

        for (TestHeader header : TESTCASES) {
            if (header.TESTCODE.equals(scenario)) {
                if (!randomOrder) {
                    for (TestData data : header.SCENARIOS) {
                        if (data.USED.isEmpty()) {
                            cases.add(data);
                            if (flagAsUsed)
                                data.USED = FLAG_CHECKED;
                            if (cases.size() == qty)
                                break;
                        }
                    }
                } else {
                    ArrayList<Integer> availableIndexes = new ArrayList<>();
                    //Determine available indexes
                    for (int i = 0; i < header.SCENARIOS.size(); i++) {
                        if (header.SCENARIOS.get(i).USED.isEmpty())
                            availableIndexes.add(i);
                    }
                    //Randomly assign available cases to output
                    while (cases.size() < qty && cases.size() < availableIndexes.size()) {
                        //Randomize case
                        int caseIndexOfIndexes = new Random().nextInt(availableIndexes.size());
                        TestData data = header.SCENARIOS.get(availableIndexes.get(caseIndexOfIndexes));
                        cases.add(data);
                        if (flagAsUsed)
                            data.USED = FLAG_CHECKED;
                        availableIndexes.remove(caseIndexOfIndexes);
                    }
                }
                break;
            }
        }
        return cases;
    }
}
