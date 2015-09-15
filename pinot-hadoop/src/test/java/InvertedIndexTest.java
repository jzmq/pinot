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
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;

/**
 * Created by dengqiang on 9/11/15.
 */
public class InvertedIndexTest {

  public static void main(String[] args) throws Exception {
    String _postfix = "abc";
    String seqId = "99";
    String dataFilePath = "/opt/pinot/audienx_index.00999.avro";


    Schema schema = new ObjectMapper().readValue(new File("/workspace/admaster/dap-service/src/main/java/com/admaster/dmp/audienx/table/audienx_index.schema.json"), Schema.class);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setTableName("coder");

    segmentGeneratorConfig.setInputFilePath(dataFilePath);

    FileFormat fileFormat = getFileFormat(dataFilePath);
    segmentGeneratorConfig.setInputFileFormat(fileFormat);
    if (null != _postfix) {
      segmentGeneratorConfig.setSegmentNamePostfix(String.format("%s-%s", _postfix, seqId));
    } else {
      segmentGeneratorConfig.setSegmentNamePostfix(seqId);
    }
    segmentGeneratorConfig.setRecordeReaderConfig(getReaderConfig(fileFormat));

    segmentGeneratorConfig.setIndexOutputDir("/tmp/PinotTempDirectory");
    segmentGeneratorConfig.createInvertedIndexForAllColumns();
    segmentGeneratorConfig.setCreateInvertedIndexForColumn("admckid", false);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
    // Tar the segment directory into file.
    //String segmentName = (new File(_localDiskSegmentDirectory).listFiles()[0]).getName();
    //String localSegmentPath = new File(_localDiskSegmentDirectory, segmentName).getAbsolutePath();
  }

  private static RecordReaderConfig getReaderConfig(FileFormat fileFormat) {
    RecordReaderConfig readerConfig = null;
    switch (fileFormat) {
      case CSV:
        readerConfig = new CSVRecordReaderConfig();
        break;
      case AVRO:
        break;
      case JSON:
        break;
      default:
        break;
    }
    return readerConfig;
  }

  private static FileFormat getFileFormat(String dataFilePath) {
    if (dataFilePath.endsWith(".json")) {
      return FileFormat.JSON;
    }
    if (dataFilePath.endsWith(".csv")) {
      return FileFormat.CSV;
    }
    if (dataFilePath.endsWith(".avro")) {
      return FileFormat.AVRO;
    }
    throw new RuntimeException("Not support file format - " + dataFilePath);
  }
}
