package com.shoreviewanalytics.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;

public class dbDDL {
    private final CqlSession session;

    public dbDDL(CqlSession session) {
        this.session = session;
    }

    public void createKeyspace(String keyspaceName, int numberOfReplicas) {
        CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists()
                .withSimpleStrategy(numberOfReplicas);

        session.execute(createKeyspace.build());
    }

    public void useKeyspace(String keyspace) {
        session.execute("USE " + CqlIdentifier.fromCql(keyspace));
    }

    public void createContinentTable(String keyspace, String tablename) {
        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tablename).ifNotExists()
                .withPartitionKey("recordid", DataTypes.INT)
                .withColumn("continent", DataTypes.TEXT);

        executeStatement(createTable.build(), keyspace);
    }
    public void createVideoTable(String keyspace, String tablename) {

        CreateTableWithOptions createTable = SchemaBuilder.createTable(keyspace, tablename).ifNotExists()
                .withPartitionKey("title", DataTypes.TEXT)
                .withPartitionKey("added_year",DataTypes.INT)
                .withColumn("added_date",DataTypes.TIMESTAMP)
                .withColumn("description", DataTypes.TEXT)
                .withColumn("user_id",DataTypes.UUID)
                .withColumn("video_id",DataTypes.TIMEUUID);

        executeStatement(createTable.build(), keyspace);
    }

    private ResultSet executeStatement(SimpleStatement statement, String keyspace) {
        if (keyspace != null) {
            statement.setKeyspace(CqlIdentifier.fromCql(keyspace));
        }
        return session.execute(statement);
    }

}
