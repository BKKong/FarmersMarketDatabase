package org.kbiying.farmersmarket.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class FarmersMarketServer {

  private static final Logger logger = Logger.getLogger(FarmersMarketServer.class.getName());

  private final Server server;

  public FarmersMarketServer(FarmersMarketServerOptions options) throws SQLException {
    this.server = ServerBuilder.forPort(options.getPort())
        .addService(new FarmersMarketService(options.getSqliteDbPath()))
        .build();
  }

  public void start() throws IOException {
    server.start();
    logger.info("Server started, listening on port " + server.getPort() + "...");
  }

  public void shutdown() {
    logger.info("Server shutting down...");
    server.shutdown();
  }

  public void awaitTermination() throws InterruptedException {
    server.awaitTermination();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  public static void main(String[] args) {
    try {
      FarmersMarketServerOptions serverOptions = getServerOptions(args);
      FarmersMarketServer server = new FarmersMarketServer(serverOptions);
      server.start();
      server.addShutdownHook();
      server.awaitTermination();
    } catch (InterruptedException | IOException | ParseException | SQLException e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  private static FarmersMarketServerOptions getServerOptions(String[] args) throws ParseException {
    CommandLine commandLine = parseCommandLine(args);
    int port = ((Number) commandLine.getParsedOptionValue("port")).intValue();
    String sqliteDbPath = commandLine.getOptionValue("db");
    return FarmersMarketServerOptions.of(port, sqliteDbPath);
  }

  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    Option port = Option.builder()
        .argName("port")
        .longOpt("port")
        .hasArg()
        .type(Number.class)
        .required()
        .build();
    Option sqliteDbPath = Option.builder()
        .argName("db")
        .longOpt("db")
        .hasArg()
        .required()
        .build();
    Options options = new Options();
    options.addOption(port);
    options.addOption(sqliteDbPath);
    CommandLineParser parser = new DefaultParser();
    return parser.parse(options, args);
  }
}
