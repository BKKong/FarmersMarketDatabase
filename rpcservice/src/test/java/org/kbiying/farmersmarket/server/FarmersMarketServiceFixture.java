package org.kbiying.farmersmarket.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.rules.TemporaryFolder;
import org.kbiying.farmersmarket.client.FarmersMarketClient;
import org.kbiying.farmersmarket.client.FarmersMarketServerAddress;

class FarmersMarketServiceFixture extends TemporaryFolder {

  private static final Logger logger = Logger
      .getLogger(FarmersMarketServiceFixture.class.getName());

  private static final FarmersMarketServerAddress SERVER_ADDRESS = FarmersMarketServerAddress
      .of("localhost", 7777);
  private static final Duration CLIENT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

  private String sqliteDbPath;
  private FarmersMarketServer server;
  private FarmersMarketClient client;

  FarmersMarketClient getClient() {
    return client;
  }

  void resetDb() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteDbPath);
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("DROP TABLE IF EXISTS Markets");
      statement.executeUpdate("CREATE TABLE Markets ("
          + "Id INTEGER PRIMARY KEY AUTOINCREMENT, "
          + "Name TEXT NOT NULL, "
          + "Address TEXT, "
          + "City TEXT, "
          + "County TEXT, "
          + "State TEXT, "
          + "Zip TEXT, "
          + "Lat REAL, "
          + "Long REAL)");
    }
  }

  @Override
  protected void before() throws Throwable {
    super.before();
    sqliteDbPath = newFile().getAbsolutePath();
    resetDb();
    server = new FarmersMarketServer(
        FarmersMarketServerOptions.of(SERVER_ADDRESS.getPort(), sqliteDbPath));
    server.start();
    client = new FarmersMarketClient(SERVER_ADDRESS);
  }

  @Override
  protected void after() {
    client.shutdown(CLIENT_SHUTDOWN_TIMEOUT);
    server.shutdown();
    try {
      server.awaitTermination();
    } catch (InterruptedException e) {
      logger.log(Level.SEVERE, e.getMessage(), e);
    }
    super.after();
  }
}
