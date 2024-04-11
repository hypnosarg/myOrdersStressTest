import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.olingo.odata2.api.commons.HttpStatusCodes;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntityContainer;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.processor.ODataResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class OdataClient {
    private static CloseableHttpClient m_httpClient = null;
    private static Edm m_edm = null;
    private static String m_csrfToken = null;
    public static final boolean PRINT_RAW_CONTENT = true;
    public static final String HTTP_METHOD_PUT = "PUT";
    public static final String HTTP_METHOD_MERGE = "MERGE";
    public static final String HTTP_METHOD_PATCH = "PATCH";
    public static final String HTTP_METHOD_POST = "POST";
    public static final String HTTP_METHOD_GET = "GET";
    private static final String HTTP_METHOD_DELETE = "DELETE";

    public static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HTTP_HEADER_ACCEPT = "Accept";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String CSRF_TOKEN_HEADER = "X-CSRF-Token";
    public static final String HEADER_CONNECTION = "Connection";
    public static final String KEEP_ALIVE = "Keep-Alive";
    public static final String CSRF_TOKEN_FETCH = "Fetch";
    public static final String APPLICATION_JSON = "application/json";
    private boolean useJson = false;
    public static final String APPLICATION_XML = "application/xml";
    public static final String APPLICATION_ATOM_XML = "application/atom+xml";
    public static final String APPLICATION_FORM = "application/x-www-form-urlencoded";
    public static final String METADATA = "$metadata";
    public static final String INDEX = "/index.jsp";
    public static final String SEPARATOR = "/";
    public String serviceUrl;
    private String user;
    private String password;
    public Edm edm;
    public String bearerToken;
    public OdataClient(String inServiceUrl,String inUser, String inPassword) throws Exception{
        serviceUrl = inServiceUrl;
        user = inUser;
        password = inPassword;
        try {
            edm = readEdm();
        } catch (IOException|ODataException e) {
            throw  new Exception("Error getting EDM, check auth and connection");
        }

    }
    public OdataClient(String inServiceUrl,String inToken) throws Exception{
        serviceUrl = inServiceUrl;
        bearerToken = inToken;
        try {
            edm = readEdm();
        } catch (IOException|ODataException e) {
            throw  new Exception("Error getting EDM, check auth and connection");
        }

    }
    public void enableJson(Boolean value){
        useJson = value;
    }
    public String getContentType(){
        if (useJson)
            return APPLICATION_JSON;
            else
                return  APPLICATION_XML;
    }

    private String getAuthorizationHeaderUserPass(){
        // Note: This example uses Basic Authentication
        // Preferred option is to use OAuth SAML bearer flow.
        String temp = new StringBuilder(user).append(":")
                .append(password).toString();
        String result = "Basic "
                + new String(Base64.getEncoder().encode(temp.getBytes()));
        return result;
    }
    private String getauthorizationheaderToken() {
        // Note: This example uses Basic Authentication
        // Preferred option is to use OAuth SAML bearer flow.
        return "Bearer " + bearerToken;
    }
    private String getAuthorizationHeader() {
       if (bearerToken.isEmpty()){
           return getAuthorizationHeaderUserPass();
       }else {
           return getauthorizationheaderToken();
       }
    }
    public ODataEntry readEntry(Edm edm, String serviceUri, String contentType, String entitySetName, String keyValue)
            throws IOException, ODataException {
        // working with the default entity container
        EdmEntityContainer entityContainer = edm.getDefaultEntityContainer();
        // create absolute uri based on service uri, entity set name and key property value
        String absolutUri = createUri(serviceUri, entitySetName, keyValue,null);

        InputStream content = execute(absolutUri, contentType, HTTP_METHOD_GET);

        return EntityProvider.readEntry(contentType,
                entityContainer.getEntitySet(entitySetName),
                content,
                EntityProviderReadProperties.init().build());
    }
    public Authenticator getAuthenticator(String user,String password){
        return new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user,password.toCharArray());
            }
        };
    }
    public String  authorize( )  {

        String formattedUrl = new StringBuilder(serviceUrl)
                .append(SEPARATOR).append(METADATA).toString();


        final HttpGet get = new HttpGet(formattedUrl);
        get.setHeader(AUTHORIZATION_HEADER, getAuthorizationHeader());
        get.setHeader(CSRF_TOKEN_HEADER, CSRF_TOKEN_FETCH);
        CloseableHttpResponse response;
        try {
            response = (CloseableHttpResponse) getHttpClient().execute(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return response.getFirstHeader(CSRF_TOKEN_HEADER).getValue();
//
//        m_edm = EntityProvider.readMetadata(response.getEntity().getContent(),
//                false);
//        return m_edm;
    }
    public Edm readEdm() throws IOException, ODataException {
        InputStream content = execute(serviceUrl + "/" + METADATA, APPLICATION_XML, HTTP_METHOD_GET);
        return EntityProvider.readMetadata(content, false);
    }
    private static CloseableHttpClient getHttpClient() {
        if (m_httpClient == null) {
            m_httpClient = HttpClientBuilder.create().build();
        }
        return m_httpClient;
    }
    private String createUri(String serviceUri, String entitySetName, String id,String filters) {
        final StringBuilder absolutUri = new StringBuilder(serviceUri).append("/").append(entitySetName);
        if(id != null) {
            absolutUri.append("(").append(id).append(")");
        }else if (filters != null){
            absolutUri.append("?$filter=").append(filters);
        }
        return absolutUri.toString();
    }
    private InputStream execute(String relativeUri, String contentType, String httpMethod) throws IOException {
        HttpURLConnection connection = initializeConnection(relativeUri, contentType, httpMethod);

        connection.connect();
        checkStatus(connection);

        InputStream content = connection.getInputStream();
        content = getRawContent(httpMethod + " request:\n  ", content, "\n",false);
        return content;
    }

//    private String getCSRFToken() {
//        URI metadataUri = this.client.newURIBuilder(serviceUrl).appendMetadataSegment().build();
//        ODataEntitySetIteratorRequest<ClientEntitySet, ClientEntity> request = this.client.getRetrieveRequestFactory().getEntitySetIteratorRequest(metadataUri);
//        request.addCustomHeader("X-CSRF-Token", "Fetch");
//        // here we have in fact xml rather than atom+xml
//        request.setAccept(ContentType.APPLICATION_XML.toContentTypeString());
//        ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response = request.execute();
//        return response.getHeader("X-CSRF-Token").iterator().next();
//    }

    private HttpURLConnection connect(String relativeUri, String contentType, String httpMethod) throws IOException {
        HttpURLConnection connection = initializeConnection(relativeUri, contentType, httpMethod);

        connection.connect();
        checkStatus(connection);


        return connection;
    }


    private HttpURLConnection initializeConnection(String absolutUri, String contentType, String httpMethod)
            throws MalformedURLException, IOException {
        List<String> cookies = new ArrayList<>();
        URL url = new URL(absolutUri);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty(HTTP_HEADER_ACCEPT, contentType);
        if(HTTP_METHOD_POST.equals(httpMethod) || HTTP_METHOD_PUT.equals(httpMethod)) {
            connection.setDoOutput(true);
            connection.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
            connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
            //For these operations we need the CSRF token, for this we need to make a GET first
            connection.setRequestMethod(HTTP_METHOD_GET);
            connection.setRequestProperty(CSRF_TOKEN_HEADER, CSRF_TOKEN_FETCH);
            connection.connect();
            String token = connection.getHeaderField("x-csrf-token");
            cookies = connection.getHeaderFields().get("set-cookie");
            //Set back the post and the token
            if (HttpURLConnection.HTTP_OK == connection.getResponseCode()) {
                connection = (HttpURLConnection) url.openConnection();
                for (String cookie : cookies) {
                    String tmp = cookie.split(";", 2)[0];
                    connection.addRequestProperty("Cookie", tmp);
                }
                connection.setDoOutput(true);
                connection.setRequestMethod(httpMethod);
                connection.setRequestProperty(CSRF_TOKEN_HEADER, token);
                connection.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
                connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
            }
        }else{
            connection.setRequestMethod(httpMethod);
            connection.setRequestProperty(CSRF_TOKEN_HEADER, CSRF_TOKEN_FETCH);
            connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
       //     connection.setAuthenticator(getAuthenticator(user,password));
        }
     //   connection.setAuthenticator(getAuthenticator(user,password));

        return connection;
    }

    public class ConnectionProvider{
        private HttpURLConnection connection;
        private List<String> cookies = new ArrayList<>();
        private String csrfToken;
        public ConnectionProvider(String absolutUri, String contentType) throws IOException {
            URL url = new URL(absolutUri);
            connection = (HttpURLConnection) url.openConnection();
            //Initialize the connection in general for all uses
            //(and save the all important cookies and tokens)
            connection.setRequestProperty(HTTP_HEADER_ACCEPT, contentType);
            connection.setDoOutput(true);
            connection.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
            connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
            connection.setRequestProperty(HEADER_CONNECTION, getAuthorizationHeader());
            connection.setRequestMethod(HTTP_METHOD_GET);
            //This is very important we will need the CSRF token to be able to do POST and PUT operations
            connection.setRequestProperty(CSRF_TOKEN_HEADER, CSRF_TOKEN_FETCH);
            connection.connect();
            //Save the token and cookies for next calls
            csrfToken = connection.getHeaderField("x-csrf-token");
            cookies = connection.getHeaderFields().get("set-cookie");
        }
        public HttpURLConnection prepareForGet(String absoluteUri,String contentType) throws IOException {
            URL url = new URL(absoluteUri);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            setTokenAndCookie(connection);
            connection.setRequestProperty(HTTP_HEADER_ACCEPT, contentType);
            connection.setRequestMethod(HTTP_METHOD_GET);
            connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
            connection.setAuthenticator(getAuthenticator(user,password));
            return  connection;
        }
        public HttpURLConnection prepareForPost(String absoluteUri,String contentType) throws IOException {
            URL url = new URL(absoluteUri);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            setTokenAndCookie(connection);
            connection.setDoOutput(true);
            connection.setRequestMethod(HTTP_METHOD_POST);
            connection.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
            connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
            return connection;
        }
        public HttpURLConnection prepareForPatch(String absoluteUri,String contentType) throws IOException {
            HttpURLConnection connection = prepareForPost(absoluteUri,contentType);
            connection.setRequestProperty("X-HTTP-Method-Override", HTTP_METHOD_PATCH);
            return connection;
        }
        public HttpURLConnection prepareForMerge(String absoluteUri,String contentType) throws IOException {
            HttpURLConnection connection = prepareForPost(absoluteUri,contentType);
            connection.setRequestProperty("X-HTTP-Method-Override", HTTP_METHOD_MERGE);
            return connection;
        }
        public HttpURLConnection prepareForPut(String absoluteUri,String contentType) throws IOException {
            URL url = new URL(absoluteUri);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            setTokenAndCookie(connection);
            connection.setDoOutput(true);
            connection.setRequestMethod(HTTP_METHOD_PUT);
            connection.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
            connection.setRequestProperty(AUTHORIZATION_HEADER, getAuthorizationHeader());
            return connection;
        }
        protected void setTokenAndCookie(HttpURLConnection connection){
            for (String cookie : cookies) {
                String tmp = cookie.split(";", 2)[0];
                connection.addRequestProperty("Cookie", tmp);
            }
            connection.setRequestProperty(CSRF_TOKEN_HEADER, csrfToken);
        }
//        public HttpURLConnection getConnection(){
//            return connection;
//        }
    }

    private HttpStatusCodes checkStatus(HttpURLConnection connection) throws IOException {
        HttpStatusCodes httpStatusCode = HttpStatusCodes.fromStatusCode(connection.getResponseCode());
        if (400 <= httpStatusCode.getStatusCode() && httpStatusCode.getStatusCode() <= 599) {
            throw new RuntimeException("Http Connection failed with status " + httpStatusCode.getStatusCode() + " " + httpStatusCode.toString());
        }
        return httpStatusCode;
    }
    private InputStream getRawContent(String prefix, InputStream content, String postfix, Boolean log) throws IOException {
        if(PRINT_RAW_CONTENT) {
            byte[] buffer = streamToArray(content);
            if (log)
                print(prefix + new String(buffer) + postfix);
            return new ByteArrayInputStream(buffer);
        }
        return content;
    }
    private boolean performUpdate(){
        return true;
    }
    public EdmEntitySet getEntitySetFromName(String name){
        try {
            for (EdmEntitySet set:edm.getEntitySets()){
                if (set.getName().equals(name))
                    return set;
            }
        } catch (EdmException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    public HashMap<String,Object> getPropertiesOfEntitySet(String entitySetName){
        EdmEntitySet set = getEntitySetFromName(entitySetName);
        HashMap<String,Object> properties = new HashMap<>();
        try {
            for (String property:set.getEntityType().getPropertyNames()){
                Object blankValue = new Object();
                switch (set.getEntityType().getProperty(property).getType().toString()) {
                    case "Edm.String" -> blankValue = " ";
                    case "Edm.Boolean" -> blankValue = false;
                    case "Edm.Int32" -> blankValue = 0;
                    case "Edm.DateTime" -> blankValue =  Date.from(Instant.now());
                    default -> {
                    }
                }
                properties.put(property,blankValue);
            }
        } catch (EdmException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
    public Date convertTstmpToDateTime(String inTstmp){
        return Date.from(Instant.now());
    }
    public ODataEntry createEntry(String contentType,
                                  String entitySetName, Map<String, Object> data) throws Exception {
        String absolutUri = createUri(serviceUrl, entitySetName, null,null);
        return writeEntity(edm, absolutUri, entitySetName, data, contentType, HTTP_METHOD_POST);
    }
    public ArrayList<ODataEntry>  createEntries(String contentType,
                                  String entitySetName, ArrayList<Map<String, Object>> data,int maxParallel) throws Exception {

        return writeEntities(edm, entitySetName, data, contentType, HTTP_METHOD_POST, maxParallel);
    }

    public ArrayList<ODataEntry>  updateEntries(String contentType,
                              String entitySetName, ArrayList<Map<String, Object>> data,int maxParallel, String id) throws Exception {
        String absolutUri = createUri(serviceUrl, entitySetName, null,null);
        return writeEntities(edm, entitySetName, data, contentType, HTTP_METHOD_PUT, maxParallel,id);
    }

    public void updateEntry(String contentType, String entitySetName,
                            String id, Map<String, Object> data) throws Exception {
        String absolutUri = createUri(serviceUrl, entitySetName, id,null);
        writeEntity(edm, absolutUri, entitySetName, data, contentType, HTTP_METHOD_PUT);
    }

    public HttpStatusCodes deleteEntry(String entityName, String id) throws IOException {
        String absolutUri = createUri(serviceUrl, entityName, id,null);
        HttpURLConnection connection = connect(absolutUri, APPLICATION_XML, HTTP_METHOD_DELETE);
        return HttpStatusCodes.fromStatusCode(connection.getResponseCode());
    }
    private ODataEntry performWrite(String absolutUri,HttpURLConnection connection,Map<String, Object> data,
                              String contentType,EdmEntitySet entitySet,EntityProviderWriteProperties properties ) throws IOException, EdmException, EntityProviderException {
        //For each update we need to create a new post


        // serialize data into ODataResponse object
        ODataResponse response = EntityProvider.writeEntry(contentType, entitySet, data, properties);
        // get (http) entity which is for default Olingo implementation an InputStream
        Object entity = response.getEntity();
        if (entity instanceof InputStream) {
            byte[] buffer = streamToArray((InputStream) entity);
            connection.getOutputStream().write(buffer);
        }

        // if a entity is created (via POST request) the response body contains the new created entity
        HttpStatusCodes statusCode = HttpStatusCodes.fromStatusCode(connection.getResponseCode());
        if (statusCode == HttpStatusCodes.CREATED) {
            // get the content as InputStream and de-serialize it into an ODataEntry object
            InputStream content = connection.getInputStream();
            try {
                return EntityProvider.readEntry(APPLICATION_XML,
                        entitySet, content, EntityProviderReadProperties.init().build());
            }catch (Exception ex){
                //Collect errors
            }
        }
        return null;
    }
    private ArrayList<ODataEntry> writeEntities(Edm edm, String entitySetName,
                                                ArrayList<Map<String, Object>> dataArray, String contentType, String httpMethod,int maxParallel)
            throws EdmException, MalformedURLException, IOException, EntityProviderException, URISyntaxException {
      return  writeEntities(edm, entitySetName, dataArray, contentType, HTTP_METHOD_POST, maxParallel,null);
    }
    private ArrayList<ODataEntry> writeEntities(Edm edm, String entitySetName,
                                   ArrayList<Map<String, Object>> dataArray, String contentType, String httpMethod,int maxParallel, String id)
            throws EdmException, MalformedURLException, IOException, EntityProviderException, URISyntaxException {
        EdmEntityContainer entityContainer = edm.getDefaultEntityContainer();
        EdmEntitySet entitySet = entityContainer.getEntitySet(entitySetName);
        URI rootUri = new URI(entitySetName);
        ArrayList<ODataEntry> entriesReturn = new ArrayList<>();
        EntityProviderWriteProperties properties = EntityProviderWriteProperties.serviceRoot(rootUri).build();
        String absolutUri = createUri(serviceUrl, entitySetName, id,null);
        //Start performing the updates under the same session
        Semaphore addEntries = new Semaphore(1);
        Semaphore parallelUpdatesSemaphore = new Semaphore(maxParallel);
        CountDownLatch waitForAllThreads = new CountDownLatch(dataArray.size());
        ConnectionProvider provider = new ConnectionProvider(serviceUrl,contentType);
        for (Map<String, Object> data:dataArray) {
            try {
                parallelUpdatesSemaphore.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            new Thread(()->{
                ODataEntry newEntry = null;
                HttpURLConnection connection = null;
                try {
                    //ConnectionProvider provider = new ConnectionProvider(absolutUri,contentType);
                    if (httpMethod.equals(HTTP_METHOD_POST)) {
                        connection = provider.prepareForPost(absolutUri, contentType);
                    }else{
                        connection = provider.prepareForPut(absolutUri, contentType);
                    }
                    newEntry = this.performWrite(absolutUri,connection,data,contentType,entitySet,properties);
                    if (newEntry != null){
                        addEntries.acquire();
                        entriesReturn.add(newEntry);
                        addEntries.release();
                    }
                } catch (Exception e) {
                  //  throw new RuntimeException(e);
                }
                if (connection != null)
                    connection.disconnect();
                parallelUpdatesSemaphore.release();
                waitForAllThreads.countDown();
            }).start();
        }
        try {
            waitForAllThreads.await();
        } catch (InterruptedException e) {
            return null;
        }
        return entriesReturn;
    }
    private ODataEntry writeEntity(Edm edm, String absolutUri, String entitySetName,
                                   Map<String, Object> data, String contentType, String httpMethod)
            throws EdmException, MalformedURLException, IOException, EntityProviderException, URISyntaxException {

        HttpURLConnection connection = initializeConnection(absolutUri, contentType, httpMethod);

        EdmEntityContainer entityContainer = edm.getDefaultEntityContainer();
        EdmEntitySet entitySet = entityContainer.getEntitySet(entitySetName);
        URI rootUri = new URI(entitySetName);

        EntityProviderWriteProperties properties = EntityProviderWriteProperties.serviceRoot(rootUri).build();
        // serialize data into ODataResponse object
        ODataResponse response = EntityProvider.writeEntry(contentType, entitySet, data, properties);
        // get (http) entity which is for default Olingo implementation an InputStream
        Object entity = response.getEntity();
        if (entity instanceof InputStream) {
            byte[] buffer = streamToArray((InputStream) entity);
            // just for logging
            String content = new String(buffer);
            print(httpMethod + " request on uri '" + absolutUri + "' with content:\n  " + content + "\n");
            //
            connection.getOutputStream().write(buffer);
        }

        // if a entity is created (via POST request) the response body contains the new created entity
        ODataEntry entry = null;
        HttpStatusCodes statusCode = HttpStatusCodes.fromStatusCode(connection.getResponseCode());
        if(statusCode == HttpStatusCodes.CREATED) {
            // get the content as InputStream and de-serialize it into an ODataEntry object
            InputStream content = connection.getInputStream();
            content = getRawContent(httpMethod + " request on uri '" + absolutUri + "' with content:\n  ", content, "\n",false);
            entry = EntityProvider.readEntry(contentType,
                    entitySet, content, EntityProviderReadProperties.init().build());
        }

        //
        connection.disconnect();

        return entry;
    }
    private byte[] streamToArray(InputStream stream) throws IOException {
        byte[] result = new byte[0];
        byte[] tmp = new byte[8192];
        int readCount = stream.read(tmp);
        while(readCount >= 0) {
            byte[] innerTmp = new byte[result.length + readCount];
            System.arraycopy(result, 0, innerTmp, 0, result.length);
            System.arraycopy(tmp, 0, innerTmp, result.length, readCount);
            result = innerTmp;
            readCount = stream.read(tmp);
        }
        stream.close();
        return result;
    }
    public static String getFiltersString(Map<String,Object> filters){
        final String SPACE_CODE = "%20";
        final String QUERY_EQUALS = SPACE_CODE.concat("eq").concat(SPACE_CODE);
        final String QUERY_AND    = SPACE_CODE.concat("and").concat(SPACE_CODE);
        String formattedFilter = "";
        for (String key:filters.keySet()){
            if (!formattedFilter.isEmpty())
                formattedFilter = formattedFilter.concat(QUERY_AND);
            Object value = filters.get(key);
            if (value.getClass().getName().contains("String")){
                formattedFilter = formattedFilter.concat(key + QUERY_EQUALS + "'"+value+"'" );
            }else{
                //Apply conversions
                String valueStr = "";
                if (value.getClass().getName().contains("Date")){
                    valueStr = DateUtils.dateToEdmDateStr((Date)value);
                    formattedFilter = formattedFilter.concat(key + QUERY_EQUALS + valueStr);
                }
            }

        }
        return formattedFilter;
    }
    public ODataFeed readFeed(String entitySetName,Map<String,Object> filters)
            throws IOException, ODataException {
        EdmEntityContainer entityContainer = edm.getDefaultEntityContainer();

        //Convert filter map to String
        String formattedFilter = null;
      if (filters != null){
          formattedFilter = getFiltersString(filters);
      }

        String absolutUri = createUri(serviceUrl, entitySetName, null,formattedFilter);


        InputStream content = (InputStream) connect(absolutUri, getContentType(), HTTP_METHOD_GET).getContent();
        return EntityProvider.readFeed(getContentType(),
                entityContainer.getEntitySet(entitySetName),
                content,
                EntityProviderReadProperties.init().build());
    }
    private static void print(String content) {
        System.out.println(content);
    }

    private static String prettyPrint(ODataEntry createdEntry) {
         return prettyPrint(createdEntry.getProperties(), 0);
    }

    private static String prettyPrint(Map<String, Object> properties, int level) {
        StringBuilder b = new StringBuilder();

        Set<Map.Entry<String, Object>> entries = properties.entrySet();

        for (Map.Entry<String, Object> entry : entries) {
            intend(b, level);
            b.append(entry.getKey()).append(": ");
            Object value = entry.getValue();
            if(value instanceof Map) {
                value = prettyPrint((Map<String, Object>)value, level+1);
                b.append("\n");
            } else if(value instanceof Calendar) {
                Calendar cal = (Calendar) value;
                value = SimpleDateFormat.getInstance().format(cal.getTime());
            }
            b.append(value).append("\n");
        }
        // remove last line break
        b.deleteCharAt(b.length()-1);
        return b.toString();
    }

    private static void intend(StringBuilder builder, int intendLevel) {
        for (int i = 0; i < intendLevel; i++) {
            builder.append("  ");
        }
    }



}
