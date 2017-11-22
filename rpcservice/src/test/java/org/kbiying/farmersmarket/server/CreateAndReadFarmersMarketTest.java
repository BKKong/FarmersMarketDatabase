package org.kbiying.farmersmarket.server;

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;
import static com.google.common.truth.Truth.assertThat;

import com.google.protobuf.StringValue;
import java.sql.SQLException;
import java.util.List;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.kbiying.farmersmarket.proto.FarmersMarket;
import org.kbiying.farmersmarket.proto.FarmersMarketTemplate;

@RunWith(JUnit4.class)
public class CreateAndReadFarmersMarketTest {

  @ClassRule
  public static final FarmersMarketServiceFixture fixture = new FarmersMarketServiceFixture();

  @Before
  public void setUp() throws SQLException {
    fixture.resetDb();
  }

  @Test
  public void testCreateFarmersMarket() {
    FarmersMarket farmersMarket1 = FarmersMarket.newBuilder()
        .setName("Farmer's Market1")
        .setAddress(StringValue.newBuilder().setValue("Address"))
        .setCity(StringValue.newBuilder().setValue("City"))
        .build();
    FarmersMarketTemplate farmersMarket1Template = convertFarmersMarketToTemplate(farmersMarket1);
    FarmersMarket createdFarmersMarket = fixture.getClient().create(farmersMarket1Template);
    assertThat(createdFarmersMarket.toBuilder().setId(0).build()).isEqualTo(farmersMarket1);

    List<FarmersMarket> readFarmersMarket = fixture.getClient().read(farmersMarket1Template);
    assertThat(readFarmersMarket).containsExactly(createdFarmersMarket);


    FarmersMarket farmersMarket2 = FarmersMarket.newBuilder()
        .setName("Farmer's Market2")
        .setAddress(StringValue.newBuilder().setValue("Address2"))
        .setZip(StringValue.newBuilder().setValue("99999"))
        .build();
    FarmersMarketTemplate farmersMarket2Template = convertFarmersMarketToTemplate(farmersMarket2);
    FarmersMarket createdFarmersMarket2 = fixture.getClient().create(farmersMarket2Template);
    assertThat(createdFarmersMarket2.getId()).isNotEqualTo(createdFarmersMarket.getId());



    List<FarmersMarket> readFarmersMarket2 = fixture.getClient().read(farmersMarket2Template);
    assertThat(readFarmersMarket2).containsExactly(farmersMarket2.toBuilder()
        .setId(createdFarmersMarket2.getId()).build());


  }

  private static FarmersMarketTemplate convertFarmersMarketToTemplate(FarmersMarket farmersMarket) {
    FarmersMarketTemplate.Builder templateBuilder = FarmersMarketTemplate.newBuilder();

    templateBuilder.getNameBuilder().setValue(farmersMarket.getName());
    if (farmersMarket.hasAddress()) {
      templateBuilder.getAddressBuilder().setValue(farmersMarket.getAddress().getValue());
    }
    if (farmersMarket.hasCity()) {
      templateBuilder.getCityBuilder().setValue(farmersMarket.getCity().getValue());
    }
    if (farmersMarket.hasCounty()) {
      templateBuilder.getCountyBuilder().setValue(farmersMarket.getCounty().getValue());
    }
    if (farmersMarket.hasState()) {
      templateBuilder.getStateBuilder().setValue(farmersMarket.getState().getValue());
    }
    if (farmersMarket.hasZip()) {
      templateBuilder.getZipBuilder().setValue(farmersMarket.getZip().getValue());
    }
    if (farmersMarket.hasLat()) {
      templateBuilder.getLatBuilder().setValue(farmersMarket.getLat().getValue());
    }
    if (farmersMarket.hasLong()) {
      templateBuilder.getLongBuilder().setValue(farmersMarket.getLong().getValue());
    }

    return templateBuilder.build();
  }

}
