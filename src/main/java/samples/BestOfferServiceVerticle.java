package samples;

import hu.akarnokd.rxjava2.interop.FlowableInterop;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.RxHelper;
import io.vertx.reactivex.core.http.HttpServerRequest;
import io.vertx.reactivex.ext.web.client.HttpResponse;
import io.vertx.reactivex.ext.web.client.WebClient;
import io.vertx.reactivex.ext.web.codec.BodyCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class BestOfferServiceVerticle extends AbstractVerticle {
    private final AtomicLong requestIds = new AtomicLong();

    private static final JsonArray DEFAULT_TARGETS = new JsonArray()
        .add(new JsonObject()
            .put("host", "localhost")
            .put("port", 3000)
            .put("path", "/offer"))
        .add(new JsonObject()
            .put("host", "localhost")
            .put("port", 3001)
            .put("path", "/offer"))
        .add(new JsonObject()
            .put("host", "localhost")
            .put("port", 3002)
            .put("path", "/offer"));
    private final Logger logger = LoggerFactory.getLogger(BestOfferServiceVerticle.class);
    private List<JsonObject> targets;
    private WebClient webClient;

    @Override
    public void start(Future<Void> startFuture) {
        webClient = WebClient.create(vertx);

        targets = config()
            .getJsonArray("targets", DEFAULT_TARGETS)
            .stream()
            .map(JsonObject.class::cast)
            .collect(Collectors.toList());

        vertx.createHttpServer()
            .requestHandler(this::findBestOffer)
            .rxListen(8080)
            .subscribe((server, error) -> {
                if (error != null) {
                    logger.error("Could not start the best offer service", error);
                    startFuture.fail(error);
                } else {
                    logger.info("The best offer service is running on port 8080");
                    startFuture.complete();
                }
            });
    }

    private Single<Optional<JsonObject>> requestBiddingService(JsonObject biddingServiceConfig, String requestId) {
        return webClient
            .get(
                biddingServiceConfig.getInteger("port"),
                biddingServiceConfig.getString("host"),
                biddingServiceConfig.getString("path")
            )
            .putHeader("Client-Request-Id", requestId)
            .as(BodyCodec.jsonObject())
            .rxSend()
            .retry(1)
            .timeout(500, TimeUnit.MILLISECONDS, RxHelper.scheduler(vertx))
            .map(HttpResponse::body)
            .map(body -> {
                logger.info("#{} received offer {}", requestId, body.encodePrettily());
                return Optional.of(body);
            })
            .onErrorReturnItem(Optional.empty());
    }

    private void findBestOffer(HttpServerRequest request) {
        String requestId = String.valueOf(requestIds.getAndIncrement());

        List<Single<Optional<JsonObject>>> responses = targets.stream()
            .map(targetConfig -> requestBiddingService(targetConfig, requestId))
            .collect(Collectors.toList());

        Single.merge(responses)
            .concatMapDelayError(FlowableInterop::fromOptional)
            .sorted(Comparator.<JsonObject, Integer>comparing(j -> j.getInteger("bid")).reversed())
            .switchIfEmpty(Flowable.error(new Exception("No offer could be found for requestId=" + requestId)))
            .subscribe(
                best -> {
                    logger.info("#{} best offer: {}", requestId, best.encodePrettily());
                    request.response()
                        .putHeader("Content-Type", "application/json")
                        .end(best.encode());
                },
                error -> {
                    logger.error("#{} ends in error", requestId, error);
                    request.response()
                        .setStatusCode(502)
                        .end();
                }
            );
    }
}
