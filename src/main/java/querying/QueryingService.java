package querying;

import model.Aggregate;
import model.ErrorMessage;
import model.Message;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.AppConfig;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.util.*;
import java.util.stream.Collectors;

@Path("api")
public class QueryingService {
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final LongSerializer serializer = new LongSerializer();
    private static final Logger log = LoggerFactory.getLogger(QueryingService.class);
    private final QueryingController controller;

    public QueryingService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.hostInfo = hostInfo;
        this.controller = new QueryingController(streams, hostInfo);
    }

    @GET
    @Path("/airquality/{metricId}/aggregate/{aggregate}/history")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAirQualityHistory(
            @PathParam("metricId") final String metricId,
            @PathParam("aggregate") final String aggregate,
            @Context final UriInfo qParams) {

        // if no geohashes have been provided => 400 Bad Request
        String geohashes = qParams.getQueryParameters().getOrDefault("geohashes", Collections.singletonList("")).get(0).toLowerCase();
        if (geohashes.equals("")) {
            String errorText = "[getAirQualityHistory] You need to provide a list of comma-separated geohash prefixes";
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        // if the specified aggregate operation is not yet supported => 400 Bad Request
        String aggr_op = aggregate.toLowerCase();
        if(!AppConfig.SUPPORTED_AGGR.contains(aggr_op)) {
            String errorText = String.format("[getAirQualityHistory] aggregate %s is not yet supported", aggregate);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        String source = qParams.getQueryParameters().getOrDefault("src", Collections.singletonList("tiles")).get(0).toLowerCase();
        String resolution = qParams.getQueryParameters().getOrDefault("res", Collections.singletonList("")).get(0).toLowerCase();
        String interval = qParams.getQueryParameters().getOrDefault("interval", Collections.singletonList("")).get(0).toLowerCase();
        Boolean local = Boolean.valueOf(qParams.getQueryParameters().getOrDefault("local", Collections.singletonList("false")).get(0).toLowerCase());
        int geohashPrecision;
        long fromDate, toDate;
        try {
            geohashPrecision = Integer.parseInt(qParams.getQueryParameters().getOrDefault("gh_precision", Collections.singletonList("6")).get(0));
            fromDate = Long.parseLong(qParams.getQueryParameters().getOrDefault("from", Collections.singletonList("-1")).get(0));
            toDate = Long.parseLong(qParams.getQueryParameters().getOrDefault("to", Collections.singletonList("-1")).get(0));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(e.getMessage(), 400))
                    .build();
            throw new WebApplicationException(errorResp);
        }

        if (fromDate > System.currentTimeMillis()) {
            String errorText = "[getAirQualityHistory] fromDate cannot be set to a future date";
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        if (toDate > 0) {
            if (fromDate >= toDate) {
                String errorText = "[getAirQualityHistory] fromDate parameter should be less than toDate";
                Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorMessage(errorText, 400))
                        .build();
                System.out.println(errorText);
                throw new WebApplicationException(errorResp);
            }
        }

        // if the specified geohash precision operation is not yet supported => 400 Bad Request
        if(!AppConfig.SUPPORTED_PRECISION.contains(geohashPrecision)) {
            String errorText = String.format("[getAirQualityHistory] precision %s is not yet supported", geohashPrecision);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        TreeMap<Long, Aggregate> results;
        if (!(resolution.isEmpty()) && AppConfig.SUPPORTED_RESOLUTIONS.contains(resolution)){
//            System.out.println("[getAirQualityHistory] query with spatial predicate...");
            results = controller.solveSpatialQuery(metricId, aggr_op, Arrays.asList(geohashes.split(",")), resolution, fromDate, toDate, source, geohashPrecision, local);
        } else if (!(interval.isEmpty()) && AppConfig.SUPPORTED_INTERVALS.contains(interval)){
//            System.out.println("[getAirQualityHistory] query with spatial and time predicates...");
            results = controller.solveSpatioTemporalQuery(metricId, aggr_op, Arrays.asList(geohashes.split(",")), interval, fromDate, source, geohashPrecision, local);
        } else {
            String errorText = String.format("[getAirQualityHistory] Invalid values for resolution (%1$s) or interval (%2$s)", resolution, interval);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }
        List<String> columns = Arrays.asList("timestamp", aggr_op);
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("metric_id", metricId);
        return prepareResponse(aggr_op, results, metadata, columns, local, new GenericType<Long>(){});
    }

    @GET
    @Path("/airquality/{metricId}/aggregate/{aggregate}/snapshot")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAirQualitySnapshot(
            @PathParam("metricId") final String metricId,
            @PathParam("aggregate") final String aggregate,
            @Context final UriInfo qParams) {

        // if snap_ts is not a valid timestamp => 400 Bad Request
        long snap_ts;
        try {
            snap_ts = Long.parseLong(qParams.getQueryParameters().getOrDefault("ts", Collections.singletonList("-1")).get(0));
            if (snap_ts == -1){
                String errorText = "[getAirQualitySnapshot] You need to provide a valid timestamp in milliseconds";
                Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorMessage(errorText, 400))
                        .build();
                System.out.println(errorText);
                throw new WebApplicationException(errorResp);
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            String errorText = "[getAirQualitySnapshot] You need to provide a valid timestamp in milliseconds";
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            throw new WebApplicationException(errorResp);
        }

        // if no bbox have been provided => 400 Bad Request
        String bbox = qParams.getQueryParameters().getOrDefault("bbox", Collections.singletonList("")).get(0).toLowerCase();
        if (bbox.equals("")) {
            String errorText = "[getAirQualitySnapshot] You need to provide a set of coordinates corresponding to a valid bounding box: (N,W,S,E)";
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        List<Double> bboxCoordinates;
        try {
             bboxCoordinates = Arrays.asList(bbox.split(",")).stream().map(c -> Double.parseDouble(c)).collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
            String errorText = "[getAirQualitySnapshot] You need to provide valid double values for the bounding box coordinates";
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            throw new WebApplicationException(errorResp);
        }

        // if the specified aggregate operation is not yet supported => 400 Bad Request
        String aggr_op = aggregate.toLowerCase();
        if(!AppConfig.SUPPORTED_AGGR.contains(aggr_op)) {
            String errorText = String.format("[getAirQualitySnapshot] aggregate %s is not yet supported", aggregate);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        String source = qParams.getQueryParameters().getOrDefault("src", Collections.singletonList("tiles")).get(0).toLowerCase();
        String resolution = qParams.getQueryParameters().getOrDefault("res", Collections.singletonList("")).get(0).toLowerCase();
        Boolean local = Boolean.valueOf(qParams.getQueryParameters().getOrDefault("local", Collections.singletonList("false")).get(0).toLowerCase());
        int geohashPrecision;
        try {
            geohashPrecision = Integer.parseInt(qParams.getQueryParameters().getOrDefault("gh_precision", Collections.singletonList("6")).get(0));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(e.getMessage(), 400))
                    .build();
            throw new WebApplicationException(errorResp);
        }

        // if the specified geohash precision operation is not yet supported => 400 Bad Request
        if(!AppConfig.SUPPORTED_PRECISION.contains(geohashPrecision)) {
            String errorText = String.format("[getAirQualitySnapshot] geohash precision %s is not yet supported", geohashPrecision);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

//        System.out.println("[getAirQualityHistory] query with time predicate...");
        TreeMap <String, Aggregate> results = controller.solveTimeQuery(metricId, aggr_op, resolution, snap_ts, source, geohashPrecision, bboxCoordinates, local);
        List<String> columns = Arrays.asList("geohash", aggr_op);
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("metric_id", metricId);
        return prepareResponse(aggr_op, results, metadata, columns, local, new GenericType<String>(){});
    }

    private <T> Response prepareResponse(String aggregate, Map payload, Map metadata, List<String> columns, Boolean local, GenericType<T> keyType){
        if (!local) {
            List data = new ArrayList();
//            System.out.println("[prepareResponse] Incoming payload: " + payload);
            payload.forEach((key, value) -> {
                try {
                    data.add(Arrays.asList(key, value.getClass().getField(aggregate).get(value)));
                } catch (NoSuchFieldException | IllegalAccessException ex) {
                    ex.printStackTrace();
                    Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                            .entity(new ErrorMessage(ex.getMessage(), 400))
                            .build();
                    throw new WebApplicationException(errorResp);
                }
            });
//            System.out.println("[prepareResponse] Outgoing data: " + data);
            Message respMessage = new Message(columns, data, metadata);
            return Response.ok(respMessage).build();
        }
//                System.out.println("[queryAirQuality] sending results");
//                System.out.println(results);
        return Response.ok(new GenericEntity<Map<T, Aggregate>>(payload){}).build();
    }

    /**
     * Start an embedded Jetty Server
     * @throws Exception from jetty
     */
    public void start() throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(hostInfo.host());
        connector.setPort(hostInfo.port());
        jettyServer.addConnector(connector);

        context.start();

        try {
            jettyServer.start();
        } catch (final java.net.SocketException exception) {
            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     * @throws Exception from jetty
     */
    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}
