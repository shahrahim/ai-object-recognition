package edu.njit.app;

import edu.njit.app.model.FileInfo;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.Label;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Application {

    private static final String[] imageNameCollection = new String[]{
            "1.jpg", "2.jpg", "3.jpg", "4.jpg", "5.jpg", "6.jpg", "7.jpg", "8.jpg", "9.jpg", "10.jpg"
    };

    private static final String SCHEME = "https";
    private static final int size = 16 * 1024 * 1024;

    private static WebClient webClient;
    private static RekognitionClient rekognitionClient;
    private static SqsClient sqsClient;

    public static void main(String[] args) {
        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(size)).build();
        webClient = WebClient.builder().exchangeStrategies(strategies).build();
        rekognitionClient = RekognitionClient.builder().region(Region.US_WEST_2).build();
        sqsClient = SqsClient.builder().region(Region.US_WEST_2).build();
        recognize();
    }

    public static String recognize() {
        String s3Url = System.getenv("S3_SRC");
        List<FileInfo> infoCollection = new ArrayList<>();
        for (String imageName : imageNameCollection) {
            byte[] bytes = webClient.get()
                    .uri(builder ->
                            builder.scheme(SCHEME)
                                    .host(s3Url)
                                    .path(String.format("/%s", imageName))
                                    .build("yoo"))
                    .accept(MediaType.APPLICATION_OCTET_STREAM)
                    .retrieve()
                    .bodyToMono(byte[].class)
                    .toProcessor()
                    .block();
            infoCollection.add(new FileInfo(bytes, imageName));
        }
        recognizeCar(infoCollection);
        return "";
    }

    public static void recognizeCar(List<FileInfo> infoCollection) {
        sqsClient = SqsClient.builder().region(Region.US_WEST_2).build();
        List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
        String sqsUrl = System.getenv("SQS_URL");
        infoCollection.forEach(fileInfo -> {
            getDetectLabelsResponse(fileInfo.getBytes()).labels().forEach(label -> {
                if (isCar(label)) {
                    sqsClient.sendMessage(SendMessageRequest.builder()
                            .queueUrl(sqsUrl)
                            .messageBody(fileInfo.getFileName())
                            .build());
                }
            });
        });
    }

    private static DetectLabelsResponse getDetectLabelsResponse(byte[] bytes) {
        DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder()
                .image(getImage(bytes))
                .maxLabels(10)
                .build();
        return rekognitionClient.detectLabels(detectLabelsRequest);
    }

    private static Image getImage(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return Image
                .builder()
                .bytes(SdkBytes.fromByteBuffer(byteBuffer))
                .build();
    }

    private static Boolean isCar(Label label) {
        return label.name().equalsIgnoreCase("car")
                && label.confidence() > 90.0f;
    }


}