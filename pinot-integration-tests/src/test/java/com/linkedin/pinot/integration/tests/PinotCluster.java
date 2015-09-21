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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadUtils;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import com.linkedin.pinot.server.util.ServerTestUtils;
import com.linkedin.pinot.tools.admin.command.AbstractBaseCommand;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class PinotCluster extends ClusterTest {
  static final String ZKString = "localhost:2181";
  static final String PROJECT_ROOT = "/workspace/pinot";
  static final String HELIX_CLUSTER_NAME = "PinotTest";

  public PinotCluster() throws Exception {
    ZkStarter.startLocalZkServer(2181);
    ControllerTestUtils.startController(HELIX_CLUSTER_NAME, ZKString, ControllerTestUtils.getDefaultControllerConfiguration());
    Configuration defaultServerConfiguration = ServerTestUtils.getDefaultServerConfiguration();
    defaultServerConfiguration.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_READ_MODE, "mmap");
    ServerTestUtils.startServer(HELIX_CLUSTER_NAME, ZKString, defaultServerConfiguration);
    BrokerTestUtils.startBroker(HELIX_CLUSTER_NAME, ZKString, BrokerTestUtils.getDefaultBrokerConfiguration());

    // Create a data resource
    addSchema(new File("/opt/pinot/audienx_index.schema.json"), "audienx_index");

    // Add table to resource
    addOfflineTable("/opt/pinot/audienx_index.table.json");

    uploadSegment("/opt/pinot/audienx_index");
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
