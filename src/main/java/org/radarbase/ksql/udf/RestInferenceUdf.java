package org.radarbase.ksql.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.kafka.common.Configurable;
import org.radarbase.ksql.util.HttpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UdfDescription(name = "api_inference",
        author = "yatharthranjan",
        version = "1.0.0",
        description = "A custom function to run real time inference using REST API calls.")
public class RestInferenceUdf implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RestInferenceUdf.class);

    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private String apiUrl;

    private RestInferenceUdf() {
        httpClient = HttpClientFactory.getClient();
        objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        apiUrl = (String) configs.get("ksql.functions.api_inference.base.url");
    }

    @Udf(
            description = "Run inference in realtime by loading data from the database."
    )
    public Map<String, Object> runInference(
            String dataLoaderModule,
            String dataLoaderClass,
            String dbName,
            String projectId,
            String userId,
            String modelName,
            String modelVersion,
            String sourceId,
            Timestamp startTime,
            Timestamp endTime
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

        if (startTime==null) {
            startTime = Timestamp.from(Instant.now().minus(Duration.ofDays(1)));
        }

        if (endTime==null) {
            endTime = Timestamp.from(Instant.now());
        }

        if (modelVersion==null) {
            modelVersion = "best";
        }

        URI uri = URI
                .create(apiUrl)
                .resolve("/model/" + modelName + "/" + modelVersion + "/metadata-invocation");

        RequestBody formBody = new FormBody.Builder()
                .add("filename", dataLoaderModule)
                .add("classname", dataLoaderClass)
                .add("dbname", dbName)
                .add("starttime", startTime.toString())
                .add("endtime", endTime.toString())
                .add("projectid", projectId)
                .add("userid", userId)
                .add("sourceid", sourceId)
                .build();

        Request request;
        try {
            request = new Request.Builder()
                    .url(uri.toURL())
                    .post(formBody)
                    .build();
        } catch (MalformedURLException exc) {
            logger.warn("The Request URL was invalid: {}", exc.getMessage());
            return null;
        }

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                return objectMapper.readValue(Objects.requireNonNull(
                        response.body(), "There was no data returned."
                        ).bytes(),
                        new TypeReference<Map<String, Object>>() {
                        });
            } else {
                String resBody = null;
                if (response.body()!=null) {
                    resBody = response.body().string();
                }
                logger.warn("The request was not successful: {}: \n\tbody={}", response, resBody);
                return null;
            }
        } catch (JsonProcessingException exc) {
            logger.warn(
                    "There was an error parsing the json response: {}, {}",
                    exc.getMessage(),
                    exc.getCause().getMessage()
            );
            return null;
        } catch (IOException exc) {
            logger.warn("There was an error making request to inference api: {}", exc.getMessage());
            return null;
        }
    }

}
