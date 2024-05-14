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
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.logging.Handler;

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
    public static final String FOLDER_AND_FILE_CONFIG = "\\testCases\\config.json";
    public static final String FOLDER_AND_FILE_SYNC = "\\testCases\\device_#\\tech_download_volatile.BAT";
    public static final String FOLDER_AND_FILE_SYNC_KILL = "\\testCases\\device_#\\killsync.BAT";
    public static final String EMPTY_MODIFICATION_TST = "0 ";
    private static final int DEVICES_PER_STORE = 5;
    private static final int MAX_PARALLEL_UPDATES = 4;
    private static TokensFile tokens = null;
    private static ConfigsFile config = null;

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

        config = readConfigFile();


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
                            performSingleDeviceSimulationNew(params.totalUpdatesPerDevice, testCases, client, params.parallelUpdates, readMessagesAndNotes, performReads, deviceNo, params.cycles, params.activeDeviceCount);
                        } catch (Exception e) {

                        }
                        try {
                            mainThreadsBarrier.await();
                        } catch (Exception e) {
                            throw new RuntimeException("Error releasing barrier!");
                        }
                    }).start();
                }
                updateTestCasesFile(testCases);
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
        System.out.println(" Synchronization count: ".concat(String.valueOf(syncCount)));


    }

    private static void performParallelReads(MyOrdersTestCases cases, OdataClient client) {
        //We will read Notes & Messages as this is always done on article load in online mode, plus sales history

    }

    private static final Semaphore getCases = new Semaphore(1);
    protected static final int SYNCHRO_RELEASE_DELAY = 30;
    protected static ArrayList<Boolean> synchroSlotsN;
    protected static ArrayList<Integer> synchroSlotsCount;
    protected static ScheduledThreadPoolExecutor releaseExecutorN;
    final static ScheduledThreadPoolExecutor syncStartDelayer = new ScheduledThreadPoolExecutor(1);
    final static Semaphore startSyncSem = new Semaphore(1);
    protected static int syncCount = 0;
    private static void peformSynchronization() {
        try {
            startSyncSem.acquire();
        } catch (InterruptedException e) {
            startSyncSem.release();
            return;
        }
        syncStartDelayer.schedule(() -> {
            startSyncSem.release();
        }, 3, TimeUnit.SECONDS);

        if (synchroSlotsN == null || releaseExecutorN == null) {
            synchroSlotsN = new ArrayList<>(config.max_syncs);
            synchroSlotsCount = new ArrayList<>(config.max_syncs);
            releaseExecutorN = new ScheduledThreadPoolExecutor(config.max_syncs);
        }

        String path = getCurrentDirectory().concat(FOLDER_AND_FILE_SYNC);
        //Get a device that is ready;
        //Find a slot
        int selectedCount = 99999;
        int selectedDevice = 0;
        for (var deviceNo = 1; deviceNo <= config.max_syncs; deviceNo++) {

            try {
                Boolean available = synchroSlotsN.get(deviceNo - 1);
                if (available) {
                    int internalCount = synchroSlotsCount.get(deviceNo - 1);
                    if (internalCount <= selectedCount){
                        selectedCount = internalCount;
                        selectedDevice = deviceNo;
                    }
                }
            } catch (RuntimeException ex) {
                selectedCount = 0;
                selectedDevice = deviceNo;
                synchroSlotsN.add(selectedDevice - 1, false);
                synchroSlotsCount.add(deviceNo - 1, selectedCount + 1);
                break;
            }
        }
        syncCount++;
        if (synchroSlotsN.size() == config.max_syncs) {
            synchroSlotsN.set(selectedDevice - 1, false);
            synchroSlotsCount.set(selectedDevice - 1, selectedCount + 1);
        }

        path = path.replace("#", String.valueOf(selectedDevice));
        String windowTitle = "SYNCHRO_".concat(String.valueOf(selectedDevice));
        path = "cmd /c start \"" + windowTitle + "\" " + path;
        try {
            Runtime.
                    getRuntime().
                    exec(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //Wait 30 seconds to release the sync slot
        final String killCommand = "cmd /c taskkill /F /FI \"WindowTitle eq ".concat(windowTitle)
                .concat("\" /T");

        final String killCommandAdmin = "cmd /c taskkill /F /FI \"WindowTitle eq Administrator ".concat(windowTitle)
                .concat("\" /T");


        final int releaseSlot = selectedDevice - 1;

        releaseExecutorN.schedule(() -> {
            //Kill the window task
            try {
                Runtime.
                        getRuntime().
                        exec(killCommand);
                Runtime.
                        getRuntime().
                        exec(killCommandAdmin);

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            synchroSlotsN.set(releaseSlot, true);
        }, SYNCHRO_RELEASE_DELAY, TimeUnit.SECONDS);


    }

    private static int noSyncChecks = 0;
    private static LocalDateTime lastSync = null;
    private static LocalDateTime lastSyncCheck = null;
    private static Semaphore syncCheckSemaphore = new Semaphore(1);
    protected static boolean triggerSynchroNeeded(int activeDevices) {
        double syncs_perdevice_per_min = 1d / config.sync_interval_mins;
        try {
            syncCheckSemaphore.acquire();
        } catch (InterruptedException e) {
            syncCheckSemaphore.release();
            return false;
        }

        //We check only once every N seconds
        if (lastSyncCheck != null && !LocalDateTime.now().isAfter(lastSyncCheck.plusSeconds(config.seconds_sync_check))) {
            syncCheckSemaphore.release();
            return false;
        }
        lastSyncCheck = LocalDateTime.now();
        //First check that there are sync slots available
        if (synchroSlotsN != null && synchroSlotsN.size() == config.max_syncs) {
            Boolean avail = false;
            for (Boolean inAvail : synchroSlotsN) {
                avail = inAvail;
                if (avail) {
                    break;
                }
            }
            if (!avail) {
                syncCheckSemaphore.release();
                return false;
            }
        }
        //Some slots are available? then based on probability
        //indicate if a sync will be needed

        //Determine the chance of a sync given the check interval
        double checksPerMinute = Math.divideExact(60, config.seconds_sync_check);
        //Calculate chance per each check
        double chancePerCheck = activeDevices * syncs_perdevice_per_min / checksPerMinute;
        //And now adjust the chance taking into account how many previous failed checks
        //we had (eventually chance converges to 100%)
        double currentChance = noSyncChecks != 0 ? chancePerCheck * noSyncChecks : chancePerCheck;
        //Convert to percentage
        currentChance = currentChance * 100;
        //And roll the dice!
        Random rand = new Random();
        int dice = rand.nextInt(100);
        if (dice < currentChance) {
            noSyncChecks = 0;
            lastSync = LocalDateTime.now();
            syncCheckSemaphore.release();
            return true;
        } else {
            noSyncChecks++;
            syncCheckSemaphore.release();
            return false;
        }

    }
    private static void performSingleDeviceSimulationNew(int count, MyOrdersTestCases cases, OdataClient client, int parallelUpdates, Boolean readMessagesAndNotes, Boolean fullReads,
                                                         int deviceNumber, int cycles, int totalDevices) {
        ArrayList<Map<String, Object>> allData = new ArrayList<>();
        final Map<String, Integer> finalReadResultStats = new HashMap<>();
        finalReadResultStats.put("Total", 0);
        final Map<String, Integer> finalUpdateResultStats = new HashMap<>();
        finalReadResultStats.put("Total", 0);
        //Here we read hope and message as well as write for each article
        final Semaphore parallelUpdatesSemaphore = new Semaphore(parallelUpdates);
        /*remove test (Start)
        peformSynchronization();
        if (1 < 2){
            return;
        }
        remove test (End)*/
        for (int j = 0; j < cycles; j++) {
            CountDownLatch waitForAll = new CountDownLatch(count);
            for (int i = 0; i < count; i++) {
                CountDownLatch threadSyncLatch = new CountDownLatch(1);
                try {
                    parallelUpdatesSemaphore.acquire();
                } catch (InterruptedException ex) {

                }
                //Chance-based trigger synchronizations
                if (triggerSynchroNeeded(totalDevices)) {
                    peformSynchronization();
                }
                new Thread(() -> {
                    threadSyncLatch.countDown();
                    //Randomize the update case to be done
                    int updType = getUpdateType();
                    MyOrdersTestCases.TestData testData = getDataForTest(cases, updType, false);
                    Map<String, Object> data = client.getPropertiesOfEntitySet("OrderQuantitySet");
                    int oldQuantity = Integer.parseInt(testData.EXISTINGQTY.trim());
                    data.put("Hope", testData.HOPE);
                    data.put("OrderSession", testData.ORDERSESSION);
                    data.put("OrderDate", DateUtils.dateYYYYMMDDHHMMSSToDate(testData.ORDERABLEDATE));
                    data.put("Store", cases.STORE);
                    data.put("Werks", cases.WERKS);
                    data.put("OrderedQuantity", oldQuantity + 1);      //We just increase the quantity by one
                    data.put("OldOrderedQty", oldQuantity);
                    data.put("UpdatedFields", "OrderedQuantity");
                    data.put("Language", "FR");


                    CyclicBarrier internalAwait = new CyclicBarrier(3);
                    //Adhoc data getter
                    new Thread(() -> {
                        Map<String, Integer> adHocRes = getAdHocData(client, (String) data.get("Hope"), (String) data.get("Store"), (Date) data.get("OrderDate"), (String) data.get("OrderSession"));
                        adHocRes.forEach((stat, recordCount) -> {
                            int total;
                            try {
                                total = finalReadResultStats.get(stat);
                            } catch (RuntimeException ex) {
                                total = 0;
                            }
                            finalReadResultStats.put(stat, total + recordCount);
                        });

                        try {
                            internalAwait.await();
                        } catch (InterruptedException | BrokenBarrierException e) {

                        }
                    }).start();
                    //Modification thread

                    //Data modification (Stock, Min Rayon, In Assort, In Rao, etc.)
                    new Thread(() -> {
                        Map<String, Integer> updateRes = performUpdates(client, data, updType);
                        updateRes.forEach((stat, recordCount) -> {
                            int total;
                            try {
                                total = finalUpdateResultStats.get(stat);
                            } catch (RuntimeException ex) {
                                total = 0;
                            }
                            finalUpdateResultStats.put(stat, total + recordCount);
                        });
                        try {
                            internalAwait.await();
                        } catch (InterruptedException | BrokenBarrierException e) {

                        }
                    }).start();

                    try {
                        internalAwait.await();
                    } catch (InterruptedException | BrokenBarrierException e) {

                    }
                    parallelUpdatesSemaphore.release();
                    waitForAll.countDown();
                }).start();
                try {
                    threadSyncLatch.await();
                } catch (InterruptedException e) {

                }
            }

            try {
                waitForAll.await();
            } catch (InterruptedException ex) {
                System.out.println("Device number " + deviceNumber + "Failed! ");
            }
        }


        //Write some logs of the results
        //String resultsCreate = "Total record updates";
        String[] results = new String[1];
        results[0] = ("Device " + deviceNumber + ". Total records read: " + finalReadResultStats.get("Total") + '.');
        finalReadResultStats.forEach((stat, totalCount) -> {
            if (!stat.equals("Total")) {
                results[0] = results[0] + stat + ":  " + totalCount + " ";
            }
        });
        System.out.println(results[0]);

        results[0] = ("Device " + deviceNumber + ". Total updates: " + finalUpdateResultStats.get("Total") + '.');
        finalUpdateResultStats.forEach((stat, totalCount) -> {
            if (!stat.equals("Total")) {
                results[0] = results[0] + stat + ":  " + totalCount + " ";
            }
        });
        System.out.println(results[0]);
    }

    private static MyOrdersTestCases.TestData getDataForTest(MyOrdersTestCases cases, int scenario, boolean flagUsed) {
        try {
            //To assure no two threads process the same data
            getCases.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ArrayList<MyOrdersTestCases.TestData> testCases = null;


        switch (scenario) {
            case 1:
                //Stock
                testCases = cases.getArticlesForStock(flagUsed, 1, true);
                break;
            case 2:
                //Min rayon
                testCases = cases.getArticlesForMinRayon(flagUsed, 1, true);
                break;
            case 3:
                //In Assort
                testCases = cases.getArticlesForInAssort(true, 1, true);
                break;
            case 4:
                //In Rao
                testCases = cases.getArticlesForUpdate(flagUsed, 1, true);
                break;
            default:
                //Order
                testCases = cases.getArticlesForCreation(flagUsed, 1, true);
        }

//        updateTestCasesFile(cases);
        getCases.release();

        if (testCases == null || testCases.size() == 0) {
            testCases = cases.getArticlesForCreation(flagUsed, 1, true);
        }
        return testCases.get(0);

    }

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
        final Map<String, Integer> finalReadResultStats = new HashMap<>();
        finalReadResultStats.put("Total", 0);

        //Here we read hope and message as well as write for each article
        final Semaphore parallelUpdatesSemaphore = new Semaphore(parallelUpdates);
        CountDownLatch waitForAll = new CountDownLatch(allData.size());
        for (Map<String, Object> data : allData) {
            ArrayList<Map<String, Object>> partData = new ArrayList<>();
            partData.add(data);
            try {
                parallelUpdatesSemaphore.acquire();
            } catch (InterruptedException ex) {
                continue;
            }
            new Thread(() -> {
                CyclicBarrier internalAwait = new CyclicBarrier(3);
                //Adhoc data getter
                new Thread(() -> {
                    Map<String, Integer> adHocRes = getAdHocData(client, (String) data.get("Hope"), (String) data.get("Store"), (Date) data.get("OrderDate"), (String) data.get("OrderSession"));
                    adHocRes.forEach((stat, recordCount) -> {
                        int total;
                        try {
                            total = finalReadResultStats.get(stat);
                        } catch (RuntimeException ex) {
                            total = 0;
                        }
                        finalReadResultStats.put(stat, total + recordCount);
                    });

                    try {
                        internalAwait.await();
                    } catch (InterruptedException | BrokenBarrierException e) {

                    }
                }).start();
                //Modification thread

                //Data modification (Stock, Min Rayon, In Assort, In Rao, etc.)
                new Thread(() -> {
                    Map<String, Integer> updateRes = performUpdates(client, data);
                    updateRes.forEach((stat, recordCount) -> {
                        int total;
                        try {
                            total = finalReadResultStats.get(stat);
                        } catch (RuntimeException ex) {
                            total = 0;
                        }
                        finalReadResultStats.put(stat, total + recordCount);
                    });
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
        } catch (InterruptedException ex) {
            System.out.println("Device number " + deviceNumber + "Failed! ");
        }


        //Write some logs of the results
        //String resultsCreate = "Total record updates";
        String[] resultsRead = new String[1];
        resultsRead[0] = ("Device " + deviceNumber + "Total records read: " + finalReadResultStats.get("Total") + '.');
        finalReadResultStats.forEach((stat, totalCount) -> {
            if (!stat.equals("Total")) {
                resultsRead[0] = resultsRead[0] + stat + ":  " + totalCount + " ";
            }
        });
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
            return new ArrayList<>();
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
            return new ArrayList<>();
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
            return new ArrayList<>();
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
            return new ArrayList<>();
        }
        return feed.getEntries();
    }

    protected static int getUpdateType() {
        Random rand = new Random();
        int chance = rand.nextInt(100);
        if (chance < config.order_chance) {
            return 0; //order is the most likely case
        } else {
            if (chance < config.order_chance + config.stock_chance) {
                //Stock
                return 1;
            } else if (chance < config.order_chance + config.stock_chance + config.minrayon_chance) {
                //Min Rayon
                return 2;
            } else if (chance < config.order_chance + config.stock_chance + config.minrayon_chance + config.inassort_chance) {
                //In Asort
                return 3;
            } else {
                //In Rao
                return 4;
            }
        }
    }

    private static void updateOrderDetails(OdataClient client, Map<String, Object> data, String field) {
        Map<String, Object> entityData = client.getPropertiesOfEntitySet("OrderDetailsSet");
        entityData.put("UpdatedFields", field);
        entityData.put("Language", "FR");
        entityData.put("Hope", data.get("Hope"));
        entityData.put("Store", data.get("Store"));
        entityData.put("Werks", data.get("Werks"));
        entityData.put("OldStock", 0);
        entityData.put("Stock", 1);
        entityData.put("MinRayon", 1);
        entityData.put("OldMinRayon", 0);
        String id = "Store='".concat(String.valueOf(data.get("Store"))).concat("',")
                .concat("Hope='").concat(String.valueOf(data.get("Hope"))).concat("'");

        ArrayList<Map<String, Object>> partData = new ArrayList<>();
        partData.add(entityData);
        try {
            client.updateEntries(OdataClient.APPLICATION_JSON, "OrderDetailsSet", partData, 1, id);
        } catch (Exception e) {

        }
    }

    private static void updateArticle(OdataClient client, Map<String, Object> data, String field, Boolean value) {
        Map<String, Object> entityData = client.getPropertiesOfEntitySet("ArticleSet");
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
            client.updateEntries(OdataClient.APPLICATION_JSON, "ArticleSet", partData, 1, id);
        } catch (Exception e) {

        }
    }

    private static Map<String, Integer> performUpdates(OdataClient client, Map<String, Object> data, int updType) {
        Map<String, Integer> stats = new HashMap<>();
        ArrayList<Map<String, Object>> partData = new ArrayList<>();
        switch (updType) {
            case 1:
                //Stock
                updateOrderDetails(client, data, "Stock");
                stats.put("Stock", 1);
                break;
            case 2:
                //Min rayon
                updateOrderDetails(client, data, "MinRayon");
                stats.put("MinRayon", 1);
                break;
            case 3:
                //In Assort
                updateArticle(client, data, "InAssortment", true);
                stats.put("InAssortment", 1);
                break;
            case 4:
                //In Rao
                updateArticle(client, data, "InRao", true);
                stats.put("InRao", 1);
                break;
            default:
                //Order
                partData.add(data);
                try {
                    client.createEntries(OdataClient.APPLICATION_JSON, "OrderQuantitySet", partData, 1);
                    stats.put("Orders", 1);
                } catch (Exception e) {
                }
        }
        stats.put("Total", 1);
        return stats;
    }

    private static Map<String, Integer> performUpdates(OdataClient client, Map<String, Object> data) {
        return performUpdates(client, data, getUpdateType());
    }

    private static Map<String, Integer> getAdHocData(OdataClient client, String hope, String store, Date orderDate, String session) {
        final ArrayList<ODataEntry> readMessages = new ArrayList<>();
        final ArrayList<ODataEntry> readNotes = new ArrayList<>();
        final ArrayList<ODataEntry> readSales = new ArrayList<>();
        final ArrayList<ODataEntry> readPredictedOrders = new ArrayList<>();
        int threads = 3;
        CountDownLatch internalAwait = new CountDownLatch(threads);
        //Read the messages and notes for the article before posting the order
        new Thread(() -> {
            try {
                readMessages.addAll(getMessagesForHope(client, hope, store, orderDate));
            } catch (Exception ex) {
                System.out.println("Exception during read ".concat(ex.getMessage()));
            }
            internalAwait.countDown();
        }).start();
        new Thread(() -> {
            try {
                readNotes.addAll(getNotesForHope(client, hope, store, orderDate, session));
            } catch (Exception ex) {
                    System.out.println("Exception during read ".concat(ex.getMessage()));
            }
            internalAwait.countDown();
        }).start();

        new Thread(() -> {
            try {
                readPredictedOrders.addAll(getPredictedSalesForHope(client, hope, store, orderDate));
            } catch (Exception ex) {
                System.out.println("Exception during read ".concat(ex.getMessage()));
            }
            internalAwait.countDown();
        }).start();
        int total = readMessages.size() + readNotes.size() + readPredictedOrders.size();
        Map<String, Integer> stats = new HashMap<>();
        stats.put("Total", total);
        stats.put("Notes", readNotes.size());
        stats.put("Messages", readMessages.size());
        stats.put("Predicted", readPredictedOrders.size());
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

    public static ConfigsFile readConfigFile() {
        //File is located in a subdirectory on the same folder as the executable.jar file
        String path = getCurrentDirectory().concat(FOLDER_AND_FILE_CONFIG);
        try {
            File file = new File(path);
            Scanner reader = new Scanner(file);
            String jsonRaw = "";
            while (reader.hasNextLine()) {
                jsonRaw = jsonRaw.concat(reader.nextLine());
            }
            reader.close();
            return ConfigsFile.getInstance(jsonRaw);
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return null;
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
