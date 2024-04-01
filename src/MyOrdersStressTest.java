import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.core.edm.EdmDateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class MyOrdersStressTest {
    /**
     * SAMPLE CONFIGURATION PARAMETERS
     * user=jondoe@delhaize.be
     * password=somepasword123
     * totalUpdatesPerDevice=1
     * parallelUpdates=1
     * activeDeviceCount=50
     * scenario=2
     * cycles=1
     * destination=pre
     */
    public static final String FOLDER_AND_FILE = "\\testCases\\myordersmobiletests.json";
    public static final String FOLDER_AND_FILE_TOKEN = "\\testCases\\tokens.json";
    public static final String EMPTY_MODIFICATION_TST = "0 ";
    private static final int DEVICES_PER_STORE = 5;
    private static final int MAX_PARALLEL_UPDATES = 4;
    private static TokensFile tokens = null;

    protected static class Parameters {
        public String user = "";
        public String password = "";
        public String serviceUrl = "";
        public String serviceUrlDev = "https://delhaize-dev-01-apps-orderingtoolperformance.cfapps.eu20.hana.ondemand.com/OrderingToolServiceDev";
        public String serviceUrlPre = "https://delhaize-pre-01-apps-orderingtoolperformance.cfapps.eu20.hana.ondemand.com/OrderingToolServiceDev";
        public Boolean useToken = false;
        public int totalUpdatesPerDevice = 10;
        public int parallelUpdates = 2;
        public int activeDeviceCount = 2;
        public int readMessagesAndNotes = 1;
        public int readSalesHistory = 1;
        public int scenario = 0;
        public int cycles = 1;
        public String destination = "dev";

        public Parameters(String[] args) throws RuntimeException {
            for (String arg : args) {
                //Format is <argument>=<value>
                String[] keyValue = arg.split("=");
                if (keyValue.length == 2) {
                    setField(keyValue[0], keyValue[1]);
                }
            }

            String missingParams = "";
            if (!useToken) {
                if (user.isEmpty()) missingParams = missingParams.concat("user,");
                if (password.isEmpty()) missingParams = missingParams.concat("password,");
            }


            if (destination.equals("dev"))
                serviceUrl = serviceUrlDev;
            else if (destination.equals("pre")) {
                serviceUrl = serviceUrlPre;
            } else {
                missingParams = missingParams.concat("destination, ");
            }


            if (serviceUrl.isEmpty()) missingParams = missingParams.concat("serviceUrl,");
            if (!missingParams.isEmpty())
                throw new RuntimeException("Missing mandatory parameters: ".concat(missingParams));

        }

        private void setField(String fieldName, Object value) {
            Field field;
            try {
                field = getClass().getField(fieldName);
                switch (field.getType().getName()) {
                    case "int":
                        field.set(this, Integer.parseInt((String) value));
                        break;
                    case "java.lang.Boolean":
                        field.set(this, Boolean.parseBoolean((String) value));
                        break;
                    default:
                        field.set(this, value);
                }
                //       field.set(this, field.getType().getName().equals("int") ? Integer.parseInt((String) value) : value);
            } catch (SecurityException | NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
                throw new RuntimeException("Illegal argument: ".concat(fieldName));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Parameters params = new Parameters(args);
        final MyOrdersTestCases testCases = readTestCasesFile();
        if (testCases == null || testCases.STORE == null) {
            String path = getCurrentDirectory().concat(FOLDER_AND_FILE);
            throw new RuntimeException("Test file could not be opened ".concat(path));
        }
        if (params.useToken) {
            tokens = readTokenFile();
            if (tokens == null || params.activeDeviceCount > tokens.tokens.size()) {
                throw new RuntimeException("Not enough tokens for the amount of devices! ");
            }
        }


        CyclicBarrier mainThreadsBarrier = new CyclicBarrier(params.activeDeviceCount + 1);
        switch (params.scenario) {
            case 1:   //Updates scenario
            case 2: {  //Combined update/read scenario
                for (int i = 0; i < params.activeDeviceCount; i++) {
                    final Boolean performReads = params.readSalesHistory == 1;
                    final Boolean readMessagesAndNotes = params.readMessagesAndNotes == 1;
                    final int deviceNo = i;
                    new Thread(() -> {
                        OdataClient client = null;
                        try {
                            if (params.useToken) {
                                client = new OdataClient(params.serviceUrl, tokens.tokens.get(deviceNo).access_token);
                            } else {
                                client = new OdataClient(params.serviceUrl, params.user, params.password);
                            }
                            client.enableJson(true);
                            performSingleDeviceSimulation(params.totalUpdatesPerDevice, testCases, client, params.parallelUpdates, readMessagesAndNotes, performReads, deviceNo);
                        } catch (Exception e) {

                        }
                        try {
                            mainThreadsBarrier.await();
                        } catch (Exception e) {
                            throw new RuntimeException("Error releasing barrier!");
                        }
                    }).start();
                }
            }
            break;

            case 3: {
                //Reads scenario
                for (int i = 0; i < params.activeDeviceCount; i++) {
                    new Thread(() -> {
                        OdataClient client = null;
                        try {
                            client = new OdataClient(params.serviceUrl, params.user, params.password);
                            client.enableJson(true);
                            performParallelReads(testCases, client);
                        } catch (Exception e) {

                        }
                    }).start();
                }
                break;
            }
        }

        //What for all test threads to finish
        System.out.println(params.activeDeviceCount + " Main threads triggered. Waiting for all processes to finish");
        mainThreadsBarrier.await();
        System.out.println(" All threads done!");


    }

    private static void performParallelReads(MyOrdersTestCases cases, OdataClient client) {
        //We will read Notes & Messages as this is always done on article load in online mode, plus sales history

    }

    private static final Semaphore getCases = new Semaphore(1);

    private static void performSingleDeviceSimulation(int count, MyOrdersTestCases cases, OdataClient client, int parallelUpdates, Boolean readMessagesAndNotes, Boolean fullReads,
                                              int deviceNumber) {
        //This method will perform <count> orders in parallel under a single session
        try {
            //To assure no two threads process the same data
            getCases.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ArrayList<MyOrdersTestCases.TestData> testCases = cases.getArticlesForCreation(true, count, true);
        updateTestCasesFile(cases);
        getCases.release();
        ArrayList<Map<String, Object>> allData = new ArrayList<>();

        //Collect the test cases
        for (int i = 0; i < count; i++) {
            MyOrdersTestCases.TestData testCase = testCases.get(i);
            Map<String, Object> data = client.getPropertiesOfEntitySet("OrderQuantitySet");
            int oldQuantity = Integer.parseInt(testCase.EXISTINGQTY.trim());
            data.put("Hope", testCase.HOPE);
            data.put("OrderSession", testCase.ORDERSESSION);
            data.put("OrderDate", DateUtils.dateYYYYMMDDHHMMSSToDate(testCase.ORDERABLEDATE));
            data.put("Store", cases.STORE);
            data.put("Werks", cases.WERKS);
            data.put("OrderedQuantity", oldQuantity + 1);      //We just increase the quantity by one
            data.put("OldOrderedQty", oldQuantity);
            data.put("UpdatedFields", "OrderedQuantity");
            data.put("Language", "FR");
            //Update or creation?
            allData.add(data);

        }
        //Place an orders in parallel
        final Map<String,Integer> finalReadResultStats = new HashMap<>();
        finalReadResultStats.put("Total",0);

        //Here we read hope and message as well as write for each article
        final Semaphore parallelUpdatesSemaphore = new Semaphore(parallelUpdates);
        CountDownLatch waitForAll = new CountDownLatch(allData.size());
        for (Map<String, Object> data : allData) {
            ArrayList<Map<String, Object>> partData = new ArrayList<>();
            partData.add(data);
            try {
                parallelUpdatesSemaphore.acquire();
            }catch (InterruptedException ex){
                continue;
            }
            new Thread(() -> {
                CyclicBarrier internalAwait = new CyclicBarrier(3);
                //Adhoc data getter
                new Thread(() -> {
                    Map<String,Integer>  adHocRes = getAdHocData(client, (String) data.get("Hope"), (String) data.get("Store"), (Date) data.get("OrderDate"), (String) data.get("OrderSession"));
                    adHocRes.forEach((stat, recordCount) -> {
                        int total;
                        try {
                             total = finalReadResultStats.get(stat);
                        }catch(RuntimeException ex){
                            total = 0;
                        }
                        finalReadResultStats.put(stat,total + recordCount);
                    } );

                    try {
                        internalAwait.await();
                    } catch (InterruptedException | BrokenBarrierException e) {

                    }
                }).start();
                //Modification thread

                //Data modification (Stock, Min Rayon, In Assort, In Rao, etc.)
                    new Thread(() -> {
                        Map<String,Integer>  updateRes = performUpdates(client,data);
                        updateRes.forEach((stat, recordCount) -> {
                            int total;
                            try {
                                total = finalReadResultStats.get(stat);
                            }catch(RuntimeException ex){
                                total = 0;
                            }
                            finalReadResultStats.put(stat,total + recordCount);
                        } );
                        try {
                            internalAwait.await();
                        } catch (InterruptedException | BrokenBarrierException e) {

                        }
                   }).start();

                try {
                    internalAwait.await();
                } catch (InterruptedException | BrokenBarrierException e) {

                }


            }).start();
            parallelUpdatesSemaphore.release();
            waitForAll.countDown();
        }

        try {
            waitForAll.await();
        }catch (InterruptedException ex){
            System.out.println("Device number " + deviceNumber + "Failed! ");
        }


        //Write some logs of the results
        //String resultsCreate = "Total record updates";
        String[] resultsRead = new String[1];
        resultsRead[0] = ("Device " + deviceNumber + "Total records read: " + finalReadResultStats.get("Total") + '.');
        finalReadResultStats.forEach((stat, totalCount) ->{
            if (!stat.equals("Total")){
                resultsRead[0]  = resultsRead[0] + stat + ":  " + totalCount + " ";
            }
        } );
        System.out.println(resultsRead[0]);
    }

    private static List<ODataEntry> getSalesHistoryForHope(OdataClient client, String hope, String store) {
        ODataFeed feed = null;
        try {
            EdmDateTime time = new EdmDateTime();
            Map<String, Object> filters = new HashMap<>();
            filters.put("Store", store);
            filters.put("Hope", hope);
            filters.put("Language", "FR");
            feed = client.readFeed("ItemSalesSet", filters);
        } catch (IOException | ODataException e) {
            return null;
        }
        return feed.getEntries();
    }

    private static List<ODataEntry> getPredictedSalesForHope(OdataClient client, String hope, String store, Date orderDate) {
        ODataFeed feed = null;
        try {
            EdmDateTime time = new EdmDateTime();
            Map<String, Object> filters = new HashMap<>();
            filters.put("Store", store);
            filters.put("Hope", hope);
            filters.put("Date", orderDate);
            feed = client.readFeed("PredictedOrderSet", filters);
        } catch (IOException | ODataException e) {
            return null;
        }
        return feed.getEntries();
    }

    private static List<ODataEntry> getMessagesForHope(OdataClient client, String hope, String store, Date orderDate) {
        ODataFeed feed = null;
        try {
            EdmDateTime time = new EdmDateTime();
            Map<String, Object> filters = new HashMap<>();
            filters.put("Store", store);
            filters.put("Hope", hope);
            filters.put("OrderDate", orderDate);
            filters.put("Language", "FR");
            feed = client.readFeed("HopeMessagesSet", filters);
        } catch (IOException | ODataException e) {
            return null;
        }
        return feed.getEntries();
    }

    private static List<ODataEntry> getNotesForHope(OdataClient client, String hope, String store, Date orderDate, String session) {
        ODataFeed feed = null;
        try {
            EdmDateTime time = new EdmDateTime();
            Map<String, Object> filters = new HashMap<>();
            filters.put("Store", store);
            filters.put("Hope", hope);
            filters.put("OrderDate", orderDate);
            filters.put("OrderSession", session);
            feed = client.readFeed("NoteSet", filters);
        } catch (IOException | ODataException e) {
            return null;
        }
        return feed.getEntries();
    }

    protected static int getUpdateType(Map<String, Object> data ){
        return 1;
//        Random rand = new Random();
//        int chance = rand.nextInt(100);
//        if (chance > 15) {
//            return 0; //order is the most likely case
//        }else{
//            if (chance < 10){
//                //Stock
//            }else if(chance < 12){
//                //Min Rayon
//            }else if (chance < 14){
//                //In Arsort
//            }else{
//                //In Rao
//            }
//        }
//        return 0;//Todo remove
    }
    private static void updateOrderDetails(OdataClient client, Map<String, Object> data, String field){
        Map<String, Object> entityData =  client.getPropertiesOfEntitySet("OrderDetailsSet");
        entityData.put("UpdatedFields", field);
        entityData.put("Language", "FR");
        entityData.put("Hope", data.get("Hope"));
        entityData.put("Store", data.get("Store"));
        entityData.put("Werks", data.get("Werks"));
        entityData.put("OldStock",0);
        entityData.put("Stock",1);
        entityData.put("MinRayon",1);
        entityData.put("OldMinRayon",0);
        String id = "Store='".concat(String.valueOf(data.get("Store"))).concat("',")
                .concat("Hope='").concat(String.valueOf(data.get("Hope"))).concat("'");

        ArrayList<Map<String, Object>> partData = new ArrayList<>();
        partData.add(entityData);
        try {
            client.updateEntries(OdataClient.APPLICATION_JSON,"OrderDetailsSet",partData,1,id);
        } catch (Exception e) {

        }
    }
    private static void updateArticle(OdataClient client, Map<String, Object> data, String field, Boolean value){
        Map<String, Object> entityData =  client.getPropertiesOfEntitySet("ArticleSet");
        entityData.put("UpdatedFields", field);
        entityData.put(field, value);
        entityData.put("Language", "FR");
        entityData.put("Hope", data.get("Hope"));
        entityData.put("Store", data.get("Store"));
        entityData.put("Werks", data.get("Werks"));
        String id = "Store='".concat(String.valueOf(data.get("Store"))).concat("',")
                .concat("Hope='").concat(String.valueOf(data.get("Hope"))).concat("'");

        ArrayList<Map<String, Object>> partData = new ArrayList<>();
        partData.add(entityData);
        try {
            client.updateEntries(OdataClient.APPLICATION_JSON,"ArticleSet",partData,1,id);
        } catch (Exception e) {

        }
    }
    private static Map<String,Integer>    performUpdates(OdataClient client, Map<String, Object> data ){
        Map<String,Integer> stats = new HashMap<>();
        ArrayList<Map<String, Object>> partData = new ArrayList<>();

        switch (getUpdateType(data)){
            case 1:
                 //Stock
                updateOrderDetails(client,data,"Stock");
                break;
            case 2:
                //Min rayon
                updateOrderDetails(client,data,"MinRayon");
                break;
            case 3:
                //In Assort
                updateArticle(client,data,"InAssortment",true);
                break;
            case 4:
                //In Rao
                updateArticle(client,data,"InRao",true);
                break;
            default:
                 //Order
                partData.add(data);
                try {
                    ArrayList<ODataEntry> newEntries = client.createEntries(OdataClient.APPLICATION_JSON, "OrderQuantitySet", partData, 1);
                    if (newEntries != null) {
                        stats.put("OrderQuantities",newEntries.size());
                    }
                } catch (Exception e) { }
        }

        return stats;
    }
    private static Map<String,Integer>  getAdHocData(OdataClient client, String hope, String store, Date orderDate, String session) {
        final ArrayList<ODataEntry> readMessages = new ArrayList<>();
        final ArrayList<ODataEntry> readNotes = new ArrayList<>();
        final ArrayList<ODataEntry> readSales = new ArrayList<>();
        final ArrayList<ODataEntry> readPredictedOrders = new ArrayList<>();
        int threads = 3;
        CountDownLatch internalAwait = new CountDownLatch(threads);
        //Read the messages and notes for the article before posting the order
        new Thread(() -> {
            readMessages.addAll(getMessagesForHope(client, hope, store, orderDate));
            internalAwait.countDown();
        }).start();
        new Thread(() -> {
            readNotes.addAll(getNotesForHope(client, hope, store, orderDate, session));
            internalAwait.countDown();
        }).start();

        new Thread(() -> {
            readPredictedOrders.addAll(getPredictedSalesForHope(client, hope, store, orderDate));
            internalAwait.countDown();
        }).start();
        int total = readMessages.size() + readNotes.size() + readPredictedOrders.size();
        Map<String,Integer> stats = new HashMap<>();
        stats.put("Total",total);
        stats.put("Notes",readNotes.size());
        stats.put("Messages",readMessages.size());
        stats.put("Predicted",readPredictedOrders.size());
//        //If mixed case was selected, also select some extra info on the article
//        if (fullReads) {
//            new Thread(() -> {
//                readSales.addAll(getSalesHistoryForHope(client, (String) data.get("Hope"), (String) data.get("Store")));
//                internalAwait.countDown();
//            }).start();
//        }

        return stats;

    }

    private static ArrayList<MyOrdersTestCases.TestData> getMixedUpdateAndCreateTasks(int count, MyOrdersTestCases cases, boolean flagUsed) {
        //This will simply determine a mix of test cases for more realistic testing
        //Start by getting the max quantity of each case
        ArrayList<MyOrdersTestCases.TestData> casesCreation = cases.getArticlesForCreation(flagUsed, count, true);
        ArrayList<MyOrdersTestCases.TestData> casesUpdate = cases.getArticlesForUpdate(flagUsed, count, true);

        //Now we will generate a mix of cases
        ArrayList<MyOrdersTestCases.TestData> mixedCases = new ArrayList<>();

        if (casesCreation.size() < count) {
            mixedCases.addAll(casesCreation);
            //Complete with cases for update (until possible)
            mixedCases.addAll(casesUpdate.stream().limit(count - casesCreation.size()).toList());
        } else if (casesUpdate.size() < count) {
            mixedCases.addAll(casesUpdate);
            //Complete with cases for creation (until possible)
            mixedCases.addAll(casesCreation.stream().limit(count - casesUpdate.size()).toList());
        } else {
            //Random mix
            int split = new Random().nextInt(8) + 1;   //bound to 8 and +1 to assure at least one update and one creation
            //As split number of creation cases and complete with update
            mixedCases.addAll(casesCreation.stream().limit(split).toList());
            mixedCases.addAll(casesUpdate.stream().limit(count - split).toList());
        }
        //Save file to flag cases as used
        updateTestCasesFile(cases);

        return mixedCases;
    }

    public static void updateTestCasesFile(MyOrdersTestCases cases) {
        String path = getCurrentDirectory().concat(FOLDER_AND_FILE);
        String jsonCasesFile = cases.getJson();

        File file = new File(path);
        try {
            FileWriter writer = new FileWriter(file, false);
            writer.write(jsonCasesFile);
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static TokensFile readTokenFile() {
        //File is located in a subdirectory on the same folder as the executable.jar file
        String path = getCurrentDirectory().concat(FOLDER_AND_FILE_TOKEN);
        try {
            File file = new File(path);
            Scanner reader = new Scanner(file);
            String jsonRaw = "";
            while (reader.hasNextLine()) {
                jsonRaw = jsonRaw.concat(reader.nextLine());
            }
            reader.close();
            return TokensFile.getInstance(jsonRaw);
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return null;
    }

    public static MyOrdersTestCases readTestCasesFile() {

        //File is located in a subdirectory on the same folder as the executable.jar file
        String path = getCurrentDirectory().concat(FOLDER_AND_FILE);
        try {
            File file = new File(path);
            Scanner reader = new Scanner(file);
            String jsonRaw = "";
            while (reader.hasNextLine()) {
                jsonRaw = jsonRaw.concat(reader.nextLine());
            }
            reader.close();
            //Convert Json file to an object
            return MyOrdersTestCases.getInstance(jsonRaw);
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return null;
    }

    public static String getCurrentDirectory() {
        try {
            String path = new File(MyOrdersStressTest.class.getProtectionDomain().getCodeSource().getLocation()
                    .toURI()).getPath();
            //Remove the jar package if contained
            if (path.contains(".jar")) {
                path = path.substring(0, path.lastIndexOf('\\'));
            }
            return path;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
