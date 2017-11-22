package org.kbiying.farmersmarket.client;

import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.kbiying.farmersmarket.proto.CreateFarmersMarketRequest;
import org.kbiying.farmersmarket.proto.CreateFarmersMarketResponse;
import org.kbiying.farmersmarket.proto.DeleteFarmersMarketRequest;
import org.kbiying.farmersmarket.proto.DeleteFarmersMarketResponse;
import org.kbiying.farmersmarket.proto.EchoFarmersMarketRequest;
import org.kbiying.farmersmarket.proto.EchoFarmersMarketResponse;
import org.kbiying.farmersmarket.proto.FarmersMarket;
import org.kbiying.farmersmarket.proto.FarmersMarketServiceGrpc;
import org.kbiying.farmersmarket.proto.FarmersMarketTemplate;
import org.kbiying.farmersmarket.proto.ReadFarmersMarketRequest;
import org.kbiying.farmersmarket.proto.ReadFarmersMarketResponse;
import org.kbiying.farmersmarket.proto.UpdateFarmersMarketRequest;
import org.kbiying.farmersmarket.proto.UpdateFarmersMarketResponse;

public class FarmersMarketClient {

  private static final Logger logger = Logger.getLogger(FarmersMarketClient.class.getName());

  private final ManagedChannel channel;
  private final FarmersMarketServiceGrpc.FarmersMarketServiceBlockingStub blockingStub;

  public FarmersMarketClient(FarmersMarketServerAddress serverAddress) {
    this.channel = ManagedChannelBuilder
        .forAddress(serverAddress.getHost(), serverAddress.getPort())
        .usePlaintext(true)
        .build();
    this.blockingStub = FarmersMarketServiceGrpc.newBlockingStub(channel);
  }

  public FarmersMarket echo(FarmersMarket farmersMarket) {
    EchoFarmersMarketRequest request = EchoFarmersMarketRequest.newBuilder()
        .setFarmersMarket(farmersMarket).build();
    EchoFarmersMarketResponse response = blockingStub.echoFarmersMarket(request);
    logger.log(Level.INFO, "EchoFarmersMarket({0}) = {1}", new Object[]{request, response});
    return response.getFarmersMarket();
  }

  public FarmersMarket create(FarmersMarketTemplate farmersMarketTemplate) {
    CreateFarmersMarketRequest request = CreateFarmersMarketRequest.newBuilder()
        .setFarmersMarket(farmersMarketTemplate)
        .build();
    CreateFarmersMarketResponse response = blockingStub.createFarmersMarket(request);
    logger.log(Level.INFO, "CreateFarmersMarket({0}) = {1}", new Object[]{request, response});
    return response.getFarmersMarket();
  }

  public List<FarmersMarket> delete(FarmersMarketTemplate farmersMarketTemplate) {
    DeleteFarmersMarketRequest request = DeleteFarmersMarketRequest.newBuilder()
        .setFarmersMarket(farmersMarketTemplate)
        .build();
    DeleteFarmersMarketResponse response = blockingStub.deleteFarmersMarket(request);
    logger.log(Level.INFO, "DeleteFarmersMarket({0}) = {1}", new Object[]{request, response});
    return response.getFarmersMarketList();

  }

  public List<FarmersMarket> read(FarmersMarketTemplate farmersMarketTemplate) {
    ReadFarmersMarketRequest request = ReadFarmersMarketRequest.newBuilder()
        .setFarmersMarket(farmersMarketTemplate)
        .build();
    ReadFarmersMarketResponse response = blockingStub.readFarmersMarket(request);
    logger.log(Level.INFO, "ReadFarmersMarket({0}) = {1}", new Object[]{request, response});
    return response.getFarmersMarketList();
  }

  public List<FarmersMarket> update(FarmersMarketTemplate farmersMarket,
      FarmersMarketTemplate farmersMarketConditions) {
    UpdateFarmersMarketRequest request = UpdateFarmersMarketRequest.newBuilder()
        .setFarmersMarket(farmersMarket)
        .setConditions(farmersMarketConditions)
        .build();
    UpdateFarmersMarketResponse response = blockingStub.updateFarmersMarket(request);
    logger.log(Level.INFO, "UpdateFarmersMarket({0}) = {1}", new Object[]{request, response});
    return response.getFarmersMarketList();
  }

  public void shutdown(Duration timeout) {
    try {
      channel.shutdown().awaitTermination(timeout.toNanos(), TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  public static void main(String[] args) {
    FarmersMarketServerAddress serverAddress;
    try {
      serverAddress = getServerAddress(args);
    } catch (ParseException e) {
      System.err.println("Could not parse command line: " + e.getMessage());
      System.exit(1);
      return;
    }
    FarmersMarketClient client = new FarmersMarketClient(serverAddress);
    try {
      client.delete(FarmersMarketTemplate.newBuilder()
          .setName(StringValue.newBuilder().setValue("test_farmers"))
          .build());
    } catch (StatusRuntimeException e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    } finally {
      client.shutdown(Duration.ofSeconds(5));
    }
  }

  private static FarmersMarketServerAddress getServerAddress(String[] args) throws ParseException {
    CommandLine commandLine = parseCommandLine(args);
    String host = commandLine.getOptionValue("host");
    int port = ((Number) commandLine.getParsedOptionValue("port")).intValue();
    return FarmersMarketServerAddress.of(host, port);
  }

  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    Option host = Option.builder().argName("host").longOpt("host").hasArg().required().build();
    Option port = Option.builder()
        .argName("port")
        .longOpt("port")
        .hasArg()
        .type(Number.class)
        .required()
        .build();
    Options options = new Options();
    options.addOption(host);
    options.addOption(port);
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }
}
