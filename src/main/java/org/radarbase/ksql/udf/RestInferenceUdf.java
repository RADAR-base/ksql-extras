package org.radarbase.ksql.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.common.Configurable;
import org.apache.log4j.BasicConfigurator;
import org.radarbase.ksql.util.HttpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.DataflowAnomalyAnalysis")
@UdfDescription(name = "api_inference",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "A custom function to run real time inference using REST API calls.")
public class RestInferenceUdf implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RestInferenceUdf.class);

    private final transient OkHttpClient httpClient;
    private final transient ObjectMapper objectMapper;
    private transient String apiUrl;

    public RestInferenceUdf() {
        httpClient = HttpClientFactory.getClient();
        objectMapper = new ObjectMapper();
        BasicConfigurator.configure();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        apiUrl = (String) configs.get("ksql.functions.api_inference.base.url");
    }

    @Udf(
            description = "Run inference in realtime by loading data from the database (i.e. we " +
                    "only pass metadata as params). This will return the json response as-is in " +
                    "String format. If needed, the values can be extracted in KSQL using " +
                    "the 'EXTRACTJSONFIELD' scalar function."
    )
    public String runMetadataInference(
            String dataLoaderModule,
            String dataLoaderClass,
            String dbName,
            String projectId,
            String userId,
            String modelName,
            String modelVersion,
            String sourceId,
            Double startTime,
            Double endTime,
            String metric
    ) {
        if (dataLoaderModule==null
                || dataLoaderClass==null
                || dbName==null
                || projectId==null
                || userId==null
                || modelName==null
        ) {
            logger.warn("One of the required parameters was null");
            return null;
        }

        if (modelVersion==null) {
            modelVersion = "best";
        }

        String query = "";
        if (modelVersion.equals("best") && metric!=null && !metric.isEmpty()) {
            query = "?metric=" + metric;
        }

        URI uri = URI
                .create(apiUrl)
                .resolve("/models/" + modelName + "/" + modelVersion + "/metadata-invocation" + query);

        JsonNode jsonNode = objectMapper.createObjectNode()
                .put("filename", dataLoaderModule)
                .put("classname", dataLoaderClass)
                .put("dbname", dbName)
                .put("starttime", startTime)
                .put("endtime", endTime)
                .put("project_id", projectId)
                .put("user_id", userId)
                .put("source_id", sourceId);

        String json = jsonNode.toString();

        RequestBody body = RequestBody.create(json, MediaType.parse("application/json"));
        logger.debug("Requesting {} with body {}", uri, json);

        Request request;
        try {
            request = new Request.Builder()
                    .url(uri.toURL())
                    .post(body)
                    .build();
        } catch (MalformedURLException exc) {
            logger.warn("The Request URL was invalid: {}", exc.getMessage());
            return null;
        }

        logger.debug("Going to make HTTP call");
        try (Response response = httpClient.newCall(request).execute()) {
            return handleResponse(response);
        } catch (IOException exc) {
            logger.warn("There was an error making request to invocation api: {}",
                    exc.getMessage());
            return null;
        }
    }

    private String handleResponse(Response response) throws IOException {
        if (response.isSuccessful()) {
            if (response.body()==null) {
                logger.warn("The result body was null");
                return null;
            } else {
                String resBody = response.body().string();
                logger.debug("Response Body is: {}", resBody);
                return resBody;
            }
        } else {
            logger.warn("The request was not successful: {}.", response);
            return null;
        }
    }
}
