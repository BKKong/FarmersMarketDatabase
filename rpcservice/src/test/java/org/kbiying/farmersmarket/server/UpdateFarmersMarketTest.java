package org.kbiying.farmersmarket.server;

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

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
public class UpdateFarmersMarketTest {

  @ClassRule
  public static final FarmersMarketServiceFixture fixture = new FarmersMarketServiceFixture();

  @Before
  public void setUp() throws SQLException {
    fixture.resetDb();
  }

  @Test
  public void testUpdatedFarmersMarket() {
    FarmersMarket farmersMarketOld = FarmersMarket.newBuilder()
        .setName("Farmer's Market Old")
        .setAddress(StringValue.newBuilder().setValue("Address Old"))
        .setCity(StringValue.newBuilder().setValue("City Old"))
        .setState(StringValue.newBuilder().setValue("CA Old"))
        .build();

    FarmersMarket farmersMarketChangeFields = FarmersMarket.newBuilder()
        .setName("Farmer's Market Updated")
        .setAddress(StringValue.newBuilder().setValue("Address Updated"))
        .setCity(StringValue.newBuilder().setValue("City Updated"))
        .setZip(StringValue.newBuilder().setValue("90000"))
        .build();

    FarmersMarket farmersMarketUpdated = FarmersMarket.newBuilder()
        .setName("Farmer's Market Updated")
        .setAddress(StringValue.newBuilder().setValue("Address Updated"))
        .setCity(StringValue.newBuilder().setValue("City Updated"))
        .setState(StringValue.newBuilder().setValue("CA Old"))
        .setZip(StringValue.newBuilder().setValue("90000"))
        .build();

    // Prepare the templates
    FarmersMarketTemplate marketOldTemplate = convertFarmersMarketToTemplate(farmersMarketOld);
    FarmersMarketTemplate marketChangeFieldsTemplate = convertFarmersMarketToTemplate(
        farmersMarketChangeFields);
    FarmersMarketTemplate marketUpdatedTemplate = convertFarmersMarketToTemplate(
        farmersMarketUpdated);

    // Create an "old" record
    FarmersMarket createdFarmersMarket = fixture.getClient().create(marketOldTemplate);

    // Update the "old" record using create()
    List<FarmersMarket> updatedFarmersMarket = fixture.getClient()
        .update(marketChangeFieldsTemplate, marketOldTemplate);

    // Verify if the updated market is the same as desired output
    assertThat(updatedFarmersMarket.get(0).toBuilder().setId(0).build()).isEqualTo(farmersMarketUpdated);
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
