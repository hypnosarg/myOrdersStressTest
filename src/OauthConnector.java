import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.http.HttpResponse;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

public class OauthConnector {
    protected String callback = "http://localhost:8080/";
    protected String clientId;
    protected String secret;
    protected String host;
    protected String appHost;
    protected String authUri ;
    protected String tokenUri;
    protected OAuthJSONAccessTokenResponse oAuthResponse;
    public OauthConnector(String inClientId, String clientSecret, String authHost,String inAppHost){
        clientId = inClientId;
        secret = clientSecret;
        host = authHost;
        appHost = inAppHost;
        initialize();
    }
    protected void initialize(){
        authUri  = host + "/oauth/authorize";
        tokenUri = host + "/oauth/token";
        callback = appHost + "/login/callback";
    }
    public void connect(String user,String password){
        try {
            OAuthClientRequest request = OAuthClientRequest.authorizationLocation(authUri).setClientId(clientId)
                    .setRedirectURI(callback).setResponseType("code").buildQueryMessage();
         //   BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
           // String code = br.readLine();
            request = OAuthClientRequest.tokenLocation(authUri)
                    .setGrantType(GrantType.PASSWORD)
                    .setClientId(clientId)
                    .setClientSecret(secret)
                //    .setRedirectURI(callback)
                  //  .setCode(code)
                     .setUsername(user)
                    .setPassword(password)
                    .buildBodyMessage();
            OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
            request.addHeader("Accept", "application/json");
            request.addHeader("Content-Type", "application/json");
           oAuthResponse = oAuthClient.accessToken(request,
                    OAuthJSONAccessTokenResponse.class);

        } catch (OAuthSystemException e) {
            throw new RuntimeException(e);
        //} catch (IOException e) {
          //  throw new RuntimeException(e);
        } catch (OAuthProblemException e) {
            throw new RuntimeException(e);
        }
    }
    public String callService(String user,String password)  {


        String result = null;

        try {

            //My site is set as insecure, so I set the following setting
            TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;

            SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
                    .loadTrustMaterial(null, acceptingTrustStrategy)
                    .build();

            SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);

            CloseableHttpClient httpClient = HttpClients.custom()
                    .setSSLSocketFactory(csf)
                    .build();


            /* HTTPCLIENT AND HTTPPOST OOBJECT */
            HttpPost httpPost = new HttpPost(tokenUri);

            /* OAUTH PARAMETERS ADDED TO FORM */
            List<NameValuePair> form = new ArrayList<>();
            form.add(new BasicNameValuePair("grant_type", "password"));
           //form.add(new BasicNameValuePair("scope", scope));
            form.add(new BasicNameValuePair("client_id", clientId)); // these two were missing
       //     form.add(new BasicNameValuePair("client_secret", secret));
            form.add(new BasicNameValuePair("username",user));
            form.add(new BasicNameValuePair("password",password));


            // UrlEncodedFormEntity is the standard way of building a firm submission
            // and it removes the need to manually add the "Content-Type", "application/x-www-form-urlencoded" header
            UrlEncodedFormEntity entity = new UrlEncodedFormEntity(form, Consts.UTF_8);

            httpPost.setEntity(entity);

            /* SEND AND RETRIEVE RESPONSE */
            CloseableHttpResponse response = null;
            try {
              response = httpClient.execute(httpPost);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String resp = response.toString();

            /* RESPONSE AS STRING */
            //      String result = null;
//            try {
//                result = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
        catch (Exception e) {
            // TODO: handle exception
        }
        return result;
    }

    public String getBearerToken(){
        return oAuthResponse.getAccessToken();
    }
}
