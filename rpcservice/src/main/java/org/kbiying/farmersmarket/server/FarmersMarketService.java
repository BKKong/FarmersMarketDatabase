package org.kbiying.farmersmarket.server;

import com.google.common.base.Joiner;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
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

class FarmersMarketService extends FarmersMarketServiceGrpc.FarmersMarketServiceImplBase {

  private static final Logger logger = Logger.getLogger(FarmersMarketService.class.getName());

  private final String sqliteDbPath;

  FarmersMarketService(String sqliteDbPath) throws SQLException {
    this.sqliteDbPath = sqliteDbPath;
    doSanityCheck();
  }

  @Override
  public void echoFarmersMarket(
      EchoFarmersMarketRequest request,
      StreamObserver<EchoFarmersMarketResponse> responseObserver) {
    logger.log(Level.INFO, "EchoFarmersMarket({0})", request);
    EchoFarmersMarketResponse response = EchoFarmersMarketResponse.newBuilder()
        .setFarmersMarket(request.getFarmersMarket())
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void createFarmersMarket(
      CreateFarmersMarketRequest request,
      StreamObserver<CreateFarmersMarketResponse> responseObserver) {
    logger.log(Level.INFO, "CreateFarmersMarket({0})", request);
    if (request.getFarmersMarket().hasId()) {
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription("Id must not be specified")
          .asRuntimeException());
      return;
    }
    if (!request.getFarmersMarket().hasName()) {
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription("Name must be specified")
          .asRuntimeException());
      return;
    }
    FarmersMarket farmersMarket;
    try {
      farmersMarket = runTransaction(connection -> {
        try (PreparedStatement createFarmersMarket = connection.prepareStatement(
            "INSERT INTO Markets (Name, Address, City, County, State, Zip, Lat, Long) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {

          createFarmersMarket.setString(1, request.getFarmersMarket().getName().getValue());

          if (request.getFarmersMarket().hasAddress()) {
            createFarmersMarket.setString(2, request.getFarmersMarket().getAddress().getValue());
          } else {
            createFarmersMarket.setNull(2, Types.VARCHAR);
          }

          if (request.getFarmersMarket().hasCity()) {
            createFarmersMarket.setString(3, request.getFarmersMarket().getCity().getValue());
          } else {
            createFarmersMarket.setNull(3, Types.VARCHAR);
          }

          if (request.getFarmersMarket().hasCounty()) {
            createFarmersMarket.setString(4, request.getFarmersMarket().getCounty().getValue());
          } else {
            createFarmersMarket.setNull(4, Types.VARCHAR);
          }

          if (request.getFarmersMarket().hasState()) {
            createFarmersMarket.setString(5, request.getFarmersMarket().getState().getValue());
          } else {
            createFarmersMarket.setNull(5, Types.VARCHAR);
          }

          if (request.getFarmersMarket().hasZip()) {
            createFarmersMarket.setString(6, request.getFarmersMarket().getZip().getValue());
          } else {
            createFarmersMarket.setNull(6, Types.VARCHAR);
          }

          if (request.getFarmersMarket().hasLat()) {
            createFarmersMarket.setDouble(7, request.getFarmersMarket().getLat().getValue());
          } else {
            createFarmersMarket.setNull(7, Types.DOUBLE);
          }

          if (request.getFarmersMarket().hasLong()) {
            createFarmersMarket.setDouble(8, request.getFarmersMarket().getLong().getValue());
          } else {
            createFarmersMarket.setNull(8, Types.DOUBLE);
          }

          createFarmersMarket.executeUpdate();
        }

        long id;
        try (Statement getLastInsertRowId = connection.createStatement()) {
          ResultSet resultSet = getLastInsertRowId
              .executeQuery("SELECT LAST_INSERT_ROWID() FROM Markets");
          resultSet.next();
          id = resultSet.getLong(1);
        }

        List<FarmersMarket> farmersMarketList = readFarmersMarket(
            connection,
            FarmersMarketTemplate.newBuilder()
                .setId(Int64Value.newBuilder().setValue(id))
                .build());
        return farmersMarketList.get(0);
      });
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL
          .withDescription(e.getMessage())
          .withCause(e)
          .asRuntimeException());
      return;
    }

    CreateFarmersMarketResponse response = CreateFarmersMarketResponse.newBuilder()
        .setFarmersMarket(farmersMarket)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();

  }

  @Override
  public void deleteFarmersMarket(
      DeleteFarmersMarketRequest request,
      StreamObserver<DeleteFarmersMarketResponse> responseObserver) {
    logger.log(Level.INFO, "DeleteFarmersMarket({0})", request);
    DeleteFarmersMarketResponse response;
    try {
      response = runTransaction(connection -> {
        DeleteFarmersMarketResponse.Builder responseBuilder = DeleteFarmersMarketResponse
            .newBuilder();

        List<FarmersMarket> farmersMarketsToDelete = readFarmersMarket(
            connection, request.getFarmersMarket());
        responseBuilder.addAllFarmersMarket(farmersMarketsToDelete);

        try (Statement deleteSelectedMarkets = connection.createStatement()) {
          ArrayList<Long> idList = new ArrayList<>();
          for (FarmersMarket farmersMarket : responseBuilder.getFarmersMarketList()) {
            idList.add(farmersMarket.getId());
          }
          deleteSelectedMarkets.executeUpdate(
              "DELETE FROM Markets WHERE Id IN (" + Joiner.on(',').join(idList) + ")");
        }

        return responseBuilder.build();
      });
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL
          .withDescription(e.getMessage())
          .withCause(e)
          .asRuntimeException());
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();

  }

  @Override
  public void readFarmersMarket(
      ReadFarmersMarketRequest request,
      StreamObserver<ReadFarmersMarketResponse> responseObserver) {
    logger.log(Level.INFO, "ReadFarmersMarket({0})", request);
    ReadFarmersMarketResponse response;
    try {
      response = runTransaction(connection -> {
        ReadFarmersMarketResponse.Builder responseBuilder = ReadFarmersMarketResponse
            .newBuilder();
        List<FarmersMarket> farmersMarketList = readFarmersMarket(
            connection, request.getFarmersMarket());
        responseBuilder.addAllFarmersMarket(farmersMarketList);
        return responseBuilder.build();
      });
    } catch (Exception e) {
      responseObserver.onError(Status.INTERNAL
          .withDescription(e.getMessage())
          .withCause(e)
          .asRuntimeException());
      return;
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void updateFarmersMarket(
      UpdateFarmersMarketRequest request,
      StreamObserver<UpdateFarmersMarketResponse> responseObserver) {
    logger.log(Level.INFO, "UpdateMarkersMarket({0})", request);
    if (request.getFarmersMarket().hasId()) {
      responseObserver.onError(Status.INVALID_ARGUMENT
          .withDescription("Id must not be specified")
          .asRuntimeException());
      return;
    }
    // To check
    UpdateFarmersMarketResponse response;
    try {
      response = runTransaction(connection -> {

        UpdateFarmersMarketResponse.Builder responseBuilder = UpdateFarmersMarketResponse
            .newBuilder();

        List<FarmersMarket> farmersMarketsToUpdate = readFarmersMarket(
            connection, request.getConditions());

        ArrayList<Long> idList = new ArrayList<>();
        for (FarmersMarket market : farmersMarketsToUpdate) {
          long marketId = market.getId();

          try (PreparedStatement updateFarmersMarket = connection.prepareStatement(
              "UPDATE Markets "
                  + "SET Name = ?, Address = ?, City = ?, County = ?, State = ?, "
                  + "Zip = ?, Lat = ?, Long = ? "
                  + "WHERE Id = ?")) {

            if (request.getFarmersMarket().hasName()) {
              updateFarmersMarket
                  .setString(1, request.getFarmersMarket().getName().getValue());
            } else {
              updateFarmersMarket.setString(1, market.getName());
            }

            if (request.getFarmersMarket().hasAddress()) {
              updateFarmersMarket
                  .setString(2, request.getFarmersMarket().getAddress().getValue());
            } else if (market.hasAddress()) {
              updateFarmersMarket.setString(2, market.getAddress().getValue());
            } else {
              updateFarmersMarket.setNull(2, Types.VARCHAR);
            }

            if (request.getFarmersMarket().hasCity()) {
              updateFarmersMarket
                  .setString(3, request.getFarmersMarket().getCity().getValue());
            } else if (market.hasCity()) {
              updateFarmersMarket.setString(3, market.getCity().getValue());
            } else {
              updateFarmersMarket.setNull(3, Types.VARCHAR);
            }

            if (request.getFarmersMarket().hasCounty()) {
              updateFarmersMarket
                  .setString(4, request.getFarmersMarket().getCounty().getValue());
            } else if (market.hasCounty()) {
              updateFarmersMarket.setString(4, market.getCounty().getValue());
            } else {
              updateFarmersMarket.setNull(4, Types.VARCHAR);
            }

            if (request.getFarmersMarket().hasState()) {
              updateFarmersMarket
                  .setString(5, request.getFarmersMarket().getState().getValue());
            } else if (market.hasState()) {
              updateFarmersMarket.setString(5, market.getState().getValue());
            } else {
              updateFarmersMarket.setNull(5, Types.VARCHAR);
            }

            if (request.getFarmersMarket().hasZip()) {
              updateFarmersMarket
                  .setString(6, request.getFarmersMarket().getZip().getValue());
            } else if (market.hasZip()) {
              updateFarmersMarket.setString(6, market.getZip().getValue());
            } else {
              updateFarmersMarket.setNull(6, Types.VARCHAR);
            }

            if (request.getFarmersMarket().hasLat()) {
              updateFarmersMarket
                  .setDouble(7, request.getFarmersMarket().getLat().getValue());
            } else if (market.hasLat()) {
              updateFarmersMarket.setDouble(7, market.getLat().getValue());
            } else {
              updateFarmersMarket.setNull(7, Types.DOUBLE);
            }
            if (request.getFarmersMarket().hasLong()) {
              updateFarmersMarket
                  .setDouble(8, request.getFarmersMarket().getLong().getValue());
            } else if (market.hasLong()) {
              updateFarmersMarket.setDouble(8, market.getLong().getValue());
            } else {
              updateFarmersMarket.setNull(8, Types.DOUBLE);
            }

            updateFarmersMarket.setLong(9, marketId);

            updateFarmersMarket.executeUpdate();
          }

          List<FarmersMarket> markets = readFarmersMarket(connection,
              FarmersMarketTemplate.newBuilder().setId(
                  Int64Value.newBuilder().setValue(marketId)).build());
          responseBuilder.addAllFarmersMarket(markets);

        }

        return responseBuilder.build();
      });
    } catch (
        Exception e)

    {
      responseObserver.onError(Status.INTERNAL
          .withDescription(Arrays.asList(e.getStackTrace()).toString())
          .withCause(e)
          .asRuntimeException());
      return;
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }


  private List<FarmersMarket> readFarmersMarket(Connection connection,
      FarmersMarketTemplate farmersMarketTemplate) throws SQLException {
    try (PreparedStatement readMarketStatement = connection.prepareStatement(
        "SELECT Id, Name, Address, City, County, State, Zip, Lat, Long " +
            "FROM Markets WHERE (Id = ? OR ?) AND (Name = ? OR ?) AND (Address = ? OR ?) " +
            "AND (City = ? OR ?) AND (County = ? OR ?) AND (State = ? OR ?) " +
            "AND (Zip = ? OR ?) AND (Lat = ? OR ?) AND (Long = ? OR ?)")) {
      if (farmersMarketTemplate.hasId()) {
        readMarketStatement.setLong(1, farmersMarketTemplate.getId().getValue());
        readMarketStatement.setBoolean(2, false);
      } else {
        readMarketStatement.setNull(1, Types.INTEGER);
        readMarketStatement.setBoolean(2, true);
      }
      if (farmersMarketTemplate.hasName()) {
        readMarketStatement.setString(3, farmersMarketTemplate.getName().getValue());
        readMarketStatement.setBoolean(4, false);
      } else {
        readMarketStatement.setNull(3, Types.INTEGER);
        readMarketStatement.setBoolean(4, true);
      }
      if (farmersMarketTemplate.hasAddress()) {
        readMarketStatement.setString(5, farmersMarketTemplate.getAddress().getValue());
        readMarketStatement.setBoolean(6, false);
      } else {
        readMarketStatement.setNull(5, Types.VARCHAR);
        readMarketStatement.setBoolean(6, true);
      }
      if (farmersMarketTemplate.hasCity()) {
        readMarketStatement.setString(7, farmersMarketTemplate.getCity().getValue());
        readMarketStatement.setBoolean(8, false);
      } else {
        readMarketStatement.setNull(7, Types.VARCHAR);
        readMarketStatement.setBoolean(8, true);
      }
      if (farmersMarketTemplate.hasCounty()) {
        readMarketStatement.setString(9, farmersMarketTemplate.getCounty().getValue());
        readMarketStatement.setBoolean(10, false);
      } else {
        readMarketStatement.setNull(9, Types.VARCHAR);
        readMarketStatement.setBoolean(10, true);
      }
      if (farmersMarketTemplate.hasState()) {
        readMarketStatement.setString(11, farmersMarketTemplate.getState().getValue());
        readMarketStatement.setBoolean(12, false);
      } else {
        readMarketStatement.setNull(11, Types.VARCHAR);
        readMarketStatement.setBoolean(12, true);
      }
      if (farmersMarketTemplate.hasZip()) {
        readMarketStatement.setString(13, farmersMarketTemplate.getZip().getValue());
        readMarketStatement.setBoolean(14, false);
      } else {
        readMarketStatement.setNull(13, Types.VARCHAR);
        readMarketStatement.setBoolean(14, true);
      }
      if (farmersMarketTemplate.hasLat()) {
        readMarketStatement.setDouble(15, farmersMarketTemplate.getLat().getValue());
        readMarketStatement.setBoolean(16, false);
      } else {
        readMarketStatement.setNull(15, Types.DOUBLE);
        readMarketStatement.setBoolean(16, true);
      }
      if (farmersMarketTemplate.hasLong()) {
        readMarketStatement.setDouble(17, farmersMarketTemplate.getLong().getValue());
        readMarketStatement.setBoolean(18, false);
      } else {
        readMarketStatement.setNull(17, Types.DOUBLE);
        readMarketStatement.setBoolean(18, true);
      }
      ResultSet resultSet = readMarketStatement.executeQuery();
      List<FarmersMarket> farmersMarketList = new ArrayList<>();
      while (resultSet.next()) {
        farmersMarketList.add(getFarmersMarketFromRow(resultSet));
      }
      return farmersMarketList;
    }
  }


  private void doSanityCheck() throws SQLException {
    runTransaction(connection -> {
      try (Statement statement = connection.createStatement()) {
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM Markets");
        resultSet.next();
        logger.log(Level.INFO, "Database contains {0} farmers' markets.", resultSet.getLong(1));
      }
      return null;
    });
  }

  private <T> T runTransaction(FunctionalTransaction<T> transaction) throws SQLException {
    T result;
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteDbPath)) {
      connection.setAutoCommit(false);
      try {
        result = transaction.apply(connection);
        connection.commit();
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    }
    return result;
  }

  private static FarmersMarket getFarmersMarketFromRow(ResultSet resultSet) throws SQLException {
    FarmersMarket.Builder farmersMarketBuilder = FarmersMarket.newBuilder();
    farmersMarketBuilder.setId(resultSet.getLong(1));
    farmersMarketBuilder.setName(resultSet.getString(2));
    String address = resultSet.getString(3);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setAddress(StringValue.newBuilder().setValue(address));
    }
    String city = resultSet.getString(4);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setCity(StringValue.newBuilder().setValue(city));
    }
    String county = resultSet.getString(5);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setCounty(StringValue.newBuilder().setValue(county));
    }
    String state = resultSet.getString(6);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setState(StringValue.newBuilder().setValue(state));
    }
    String zip = resultSet.getString(7);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setZip(StringValue.newBuilder().setValue(zip));
    }
    double latitude = resultSet.getDouble(8);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setLat(DoubleValue.newBuilder().setValue(latitude));
    }
    double longitude = resultSet.getDouble(9);
    if (!resultSet.wasNull()) {
      farmersMarketBuilder.setLong(DoubleValue.newBuilder().setValue(longitude));
    }
    return farmersMarketBuilder.build();
  }

  @FunctionalInterface
  private interface FunctionalTransaction<T> {

    T apply(Connection connection) throws SQLException;
  }
}
