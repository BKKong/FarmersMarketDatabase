package org.kbiying.farmersmarket.server;

import com.google.auto.value.AutoValue;

@AutoValue
abstract class FarmersMarketServerOptions {

  abstract int getPort();
  abstract String getSqliteDbPath();

  static FarmersMarketServerOptions of(int port, String sqliteDbPath) {
    return new AutoValue_FarmersMarketServerOptions(port, sqliteDbPath);
  }
}
