package org.kbiying.farmersmarket.client;

import com.google.auto.value.AutoValue;

@AutoValue
public abstract class FarmersMarketServerAddress {

  public abstract String getHost();

  public abstract int getPort();

  public static FarmersMarketServerAddress of(String host, int port) {
    return new AutoValue_FarmersMarketServerAddress(host, port);
  }
}
