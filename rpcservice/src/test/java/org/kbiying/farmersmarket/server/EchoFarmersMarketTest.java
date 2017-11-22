package org.kbiying.farmersmarket.server;

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

import com.google.protobuf.StringValue;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kbiying.farmersmarket.proto.FarmersMarket;

@RunWith(JUnit4.class)
public class EchoFarmersMarketTest {

  @ClassRule
  public static final FarmersMarketServiceFixture fixture = new FarmersMarketServiceFixture();

  @Before
  public void setUp() throws SQLException {
    fixture.resetDb();
  }

  @Test
  public void testEchoFarmersMarket() {
    FarmersMarket farmersMarket = FarmersMarket.newBuilder()
        .setId(1)
        .setName("Farmers' Market")
        .setAddress(StringValue.newBuilder().setValue("Address"))
        .build();
    assertThat(fixture.getClient().echo(farmersMarket)).isEqualTo(farmersMarket);
  }
}
