/**
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * <p>Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.cloud.bigtable.helloworld;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.sun.net.httpserver.HttpServer;
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.exporter.StackdriverExporter;
import io.opencensus.trace.samplers.Samplers;
import io.opencensus.zpages.TracezHttpHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A minimal application that connects to Cloud Bigtable using the native HBase API and performs
 * some basic operations.
 */
public class HelloWorld {
  private static final Tracer tracer = Tracing.getTracer();

  // Refer to table metadata names by byte array in the HBase API
  private static final byte[] TABLE_NAME = Bytes.toBytes("Hello-Bigtable");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("greeting");

  // Write some friendly greetings to Cloud Bigtable
  private static final String[] GREETINGS = {
    "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
  };

  private static void createTable(String projectId, String instanceId) {
    // [START connecting_to_bigtable]
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END connecting_to_bigtable]

      // [START creating_a_table]
      // Create a table with a single column family
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));

      tracer.getCurrentSpan().addAnnotation("Create table " + descriptor.getNameAsString());
      admin.createTable(descriptor);
      // [END creating_a_table]

    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  /** Connects to Cloud Bigtable, runs some basic operations and prints the results. */
  private static void doWritesAndReads(String projectId, String instanceId) {

    // [START connecting_to_bigtable]
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // [START writing_rows]
      // Retrieve the table we just created so we can do some reads and writes
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

      // Write some rows to the table
      tracer.getCurrentSpan().addAnnotation("Write some greetings to the table");
      for (int i = 0; i < GREETINGS.length; i++) {
        // Each row has a unique row key.
        //
        // Note: This example uses sequential numeric IDs for simplicity, but
        // this can result in poor performance in a production application.
        // Since rows are stored in sorted order by key, sequential keys can
        // result in poor distribution of operations across nodes.
        //
        // For more information about how to design a Bigtable schema for the
        // best performance, see the documentation:
        //
        //     https://cloud.google.com/bigtable/docs/schema-design
        String rowKey = "greeting" + i;

        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(GREETINGS[i]));
        table.put(put);
      }
      // [END writing_rows]

      // [START getting_a_row]
      // Get the first greeting by row key
      String rowKey = "greeting0";
      Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
      String greeting = Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME));
      tracer.getCurrentSpan().addAnnotation("Get a single greeting by row key: " + rowKey + " = "
          + greeting);
      // [END getting_a_row]

      // [START scanning_all_rows]
      // Now scan across all rows.
      Scan scan = new Scan();

      tracer.getCurrentSpan().addAnnotation("Scan for all greetings");
      ResultScanner scanner = table.getScanner(scan);
      for (Result row : scanner) {
        byte[] keyBytes = row.getRow();
        byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        tracer.getCurrentSpan().addAnnotation(Bytes.toString(keyBytes) + " " +
            Bytes.toString(valueBytes));
      }
      // [END scanning_all_rows]

    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void deleteTable(String projectId, String instanceId) {

    // [START connecting_to_bigtable]
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      // [END connecting_to_bigtable]

      // [START deleting_a_table]
      // Clean up by disabling and then deleting the table
      tracer.getCurrentSpan().addAnnotation("Delete the table");
      admin.disableTable(TableName.valueOf(TABLE_NAME));
      admin.deleteTable(TableName.valueOf(TABLE_NAME));
      // [END deleting_a_table]

    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static void main(String[] args) throws IOException {
    // Consult system properties to get project/instance
    String projectId = "e2e-debugging"; // requiredProperty("bigtable.projectID");
    String instanceId = "e2e-debugging"; // requiredProperty("bigtable.instanceID");

    StackdriverExporter stackdriverExporter =
        StackdriverExporter.create(GoogleCredentials.getApplicationDefault(), "e2e-debugging");
    stackdriverExporter.register(Tracing.getExportComponent().getSpanExporter());
    HttpServer server = HttpServer.create(new InetSocketAddress(8000), 10);
    server.createContext("/tracez", new TracezHttpHandler());
    server.start();

    Tracing.getExportComponent()
        .getSampledSpanStore()
        .registerSpanNamesForCollection(
            Arrays.asList(
                "Sent.google.bigtable.admin.v2.BigtableTableAdmin.CreateTable",
                "Sent.google.bigtable.admin.v2.BigtableTableAdmin.DeleteTable",
                "Sent.google.bigtable.admin.v2.BigtableTableAdmin.ListTables",
                "Sent.google.bigtable.v2.Bigtable.MutateRow",
                "Sent.google.bigtable.v2.Bigtable.ReadRows",
                "CreateTable",
                "WritesAndReads",
                "DeleteTable"));

    try (Scope ss =
        tracer
            .spanBuilderWithExplicitParent("CreateTable", null)
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      createTable(projectId, instanceId);
    }

    for (int i = 0; i < 5; i++) {
      try (Scope ss =
          tracer
              .spanBuilderWithExplicitParent("WritesAndReads", null)
              .setSampler(Samplers.probabilitySampler(0.5))
              .setRecordEvents(true)
              .startScopedSpan()) {
        doWritesAndReads(projectId, instanceId);
      }
    }

    try (Scope ss =
        tracer
            .spanBuilderWithExplicitParent("DeleteTable", null)
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      deleteTable(projectId, instanceId);
    }
  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    if (value == null) {
      throw new IllegalArgumentException("Missing required system property: " + prop);
    }
    return value;
  }
}
