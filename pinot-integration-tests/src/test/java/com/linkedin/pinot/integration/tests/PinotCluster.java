/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.broker.broker.BrokerTestUtils;
//import com.linkedin.pinot.common.ZkTestUtils;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.helper.ControllerRequestBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.server.util.ServerTestUtils;
import com.linkedin.pinot.tools.admin.command.AbstractBaseCommand;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class PinotCluster extends ClusterTest {
  static final String ZKString = "10.10.2.130:2181";
  static final String PROJECT_ROOT = "/workspace/pinot";
  static final String HELIX_CLUSTER_NAME = "PinotTest";

  public PinotCluster() throws Exception {
    SimpleZkClient zkClient = new SimpleZkClient(ZKString);
    zkClient.createConnection();

    zkClient.deletePath("/" + HELIX_CLUSTER_NAME);
    ControllerTestUtils.startController(HELIX_CLUSTER_NAME, ZKString, ControllerTestUtils.getDefaultControllerConfiguration());
    Configuration defaultServerConfiguration = ServerTestUtils.getDefaultServerConfiguration();
    defaultServerConfiguration.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_READ_MODE, "mmap");
    ServerTestUtils.startServer(HELIX_CLUSTER_NAME, ZKString, defaultServerConfiguration);
    BrokerTestUtils.startBroker(HELIX_CLUSTER_NAME, ZKString, BrokerTestUtils.getDefaultBrokerConfiguration());

    // Create a data resource
    addSchema(new File(PROJECT_ROOT + "/pinot-tools/src/main/resources/sample_data/baseball/baseball.schema"), "baseball");

    // Add table to resource
    addOfflineTable(PROJECT_ROOT + "/pinot-tools/src/main/resources/sample_data/baseball" +
            "/baseballTable.json");

    Thread.sleep(10000);
    uploadSegment(PROJECT_ROOT + "/pinot-tools/src/main/resources/sample_data/baseball/segment");
  }

  public void addOfflineTable(String tableJsonPath) throws JSONException, IOException {
    JsonNode node = new ObjectMapper().readTree(new FileInputStream(tableJsonPath));
    String res = AbstractBaseCommand.sendPostRequest(ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL)
            .forTableCreate(), node.toString());
  }

  public boolean uploadSegment(String path) throws Exception {

    File dir = new File(path);
    File[] files = dir.listFiles();

    for (File file : files) {
      if (!file.isDirectory()) {
        continue;
      }

      String srcDir = file.getAbsolutePath();

      String outFile = TarGzCompressionUtils.createTarGzOfDirectory(srcDir);
      File tgzFile = new File(outFile);
      FileUploadUtils.sendSegmentFile("localhost", ControllerTestUtils.DEFAULT_CONTROLLER_API_PORT, tgzFile
                      .getName(), new FileInputStream(tgzFile), tgzFile.length());
      FileUtils.deleteQuietly(tgzFile);
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    new PinotCluster();
  }

  @Override
  protected String getHelixClusterName() {
    return HELIX_CLUSTER_NAME;
  }
}
