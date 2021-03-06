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
package com.linkedin.pinot.index.reader;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.index.reader.impl.FixedBitWidthRowColDataFileReader;
import com.linkedin.pinot.core.util.CustomBitSet;


@Test
public class FixedBitWidthRowColDataFileReaderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedBitWidthRowColDataFileReaderTest.class);
  boolean debug = false;

  @Test
  public void testReadIntFromByteBuffer() {
    int maxBits = 1;
    while (maxBits < 32) {
      System.out.println("START MAX BITS:" + maxBits);
      int numElements = 100;
      CustomBitSet customBitSet = CustomBitSet.withBitLength(numElements
          * maxBits);
      int max = (int) Math.pow(2, maxBits);
      Random r = new Random();
      int[] values = new int[numElements];
      for (int i = 0; i < numElements; i++) {
        int value = r.nextInt(max);
        values[i] = value;
        LOGGER.info(Integer.toString(value));
        StringBuilder sb = new StringBuilder();
        for (int j = maxBits - 1; j >= 0; j--) {
          if ((value & (1 << j)) != 0) {
            sb.append("1");
            customBitSet.setBit(i * maxBits + (maxBits - j - 1));
          } else {
            sb.append("0");
          }
        }
        LOGGER.info(sb.toString());
      }
      LOGGER.info("customBitSet: " + customBitSet.toString());
      int bitPos = 0;
      for (int i = 0; i < numElements; i++) {
        bitPos = i * maxBits;
        int readInt = customBitSet.readInt(bitPos, bitPos + maxBits);
        if (readInt != values[i]) {
          readInt = customBitSet.readInt(bitPos, bitPos + maxBits);
        }
        System.out.println(i + "  Expected:" + values[i] + " Actual:" + readInt);
        Assert.assertEquals(readInt, values[i]);
      }
      System.out.println("END MAX BITS:" + maxBits);
      maxBits = maxBits + 1;

    }
  }

  /**
   * Tests only positive numbers
   *
   * @throws Exception
   */
  @Test
  public void testSingleColUnsigned() throws Exception {
    int[] maxBitArray = new int[] { 4 };

    for (int maxBits : maxBitArray) {
      String fileName = "test" + maxBits + "FixedBitWidthSingleCol";
      File file = new File(fileName);
      try {
        System.out.println("START MAX BITS:" + maxBits);
        int numElements = 100;
        CustomBitSet bitset = CustomBitSet.withBitLength(numElements * maxBits);
        int max = (int) Math.pow(2, maxBits);
        Random r = new Random();
        int[] values = new int[numElements];
        for (int i = 0; i < numElements; i++) {
          int value = r.nextInt(max);
          values[i] = value;
          for (int j = maxBits - 1; j >= 0; j--) {
            if ((value & (1 << j)) != 0) {
              bitset.setBit(i * maxBits + (maxBits - j - 1));
            }
          }
        }
        byte[] byteArray = bitset.toByteArray();

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(byteArray);
        fos.close();

        FixedBitWidthRowColDataFileReader heapReader = FixedBitWidthRowColDataFileReader.forHeap(file, numElements,
            1, new int[] { maxBits });
        for (int i = 0; i < numElements; i++) {
          int readInt = heapReader.getInt(i, 0);
          System.out.println(i + "  Expected:" + values[i] + " Actual:" + readInt);
          Assert.assertEquals(readInt, values[i]);
        }
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 0);
        heapReader.close();
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 0);

        FixedBitWidthRowColDataFileReader mmapReader = FixedBitWidthRowColDataFileReader.forMmap(file, numElements,
            1, new int[] { maxBits });
        for (int i = 0; i < numElements; i++) {
          int readInt = mmapReader.getInt(i, 0);
          System.out.println(i + "  Expected:" + values[i] + " Actual:" + readInt);
          Assert.assertEquals(readInt, values[i]);
        }
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 2);
        mmapReader.close();
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 0);

        System.out.println("END MAX BITS:" + maxBits);
      } finally {
        file.delete();

      }

    }
  }

  /**
   * Tests both positive and negative numbers
   *
   * @throws Exception
   */
  @Test
  public void testSingleColSigned() throws Exception {
    int[] maxBitArray = new int[] { 4 };

    for (int maxBits : maxBitArray) {
      String fileName = "test" + maxBits + "FixedBitWidthSingleCol";
      File file = new File(fileName);
      try {
        System.out.println("START MAX BITS:" + maxBits);
        int numElements = 100;
        int requiredBits = maxBits + 1;
        CustomBitSet bitset = CustomBitSet.withBitLength(numElements
            * requiredBits);
        int max = (int) Math.pow(2, maxBits);
        Random r = new Random();
        int[] values = new int[numElements];
        int offset = max - 1;
        for (int i = 0; i < numElements; i++) {
          int value = r.nextInt(max);
          if (Math.random() > .5) {
            value = -1 * value;
          }
          values[i] = value;
          int offsetValue = offset + value;
          for (int j = requiredBits - 1; j >= 0; j--) {
            if ((offsetValue & (1 << j)) != 0) {
              bitset.setBit(i * requiredBits + (requiredBits - j - 1));
            }
          }
        }
        byte[] byteArray = bitset.toByteArray();

        FileOutputStream fos = new FileOutputStream(file);
        fos.write(byteArray);
        fos.close();

        FixedBitWidthRowColDataFileReader heapReader = FixedBitWidthRowColDataFileReader.forHeap(file, numElements,
            1, new int[] { maxBits }, new boolean[] { true });
        for (int i = 0; i < numElements; i++) {
          int readInt = heapReader.getInt(i, 0);
          System.out.println(i + "  Expected:" + values[i] + " Actual:" + readInt);
          Assert.assertEquals(readInt, values[i]);
        }
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 0);
        heapReader.close();
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 0);

        FixedBitWidthRowColDataFileReader mmapReader = FixedBitWidthRowColDataFileReader.forMmap(file, numElements,
            1, new int[] { maxBits }, new boolean[] { true });
        for (int i = 0; i < numElements; i++) {
          int readInt = mmapReader.getInt(i, 0);
          System.out.println(i + "  Expected:" + values[i] + " Actual:" + readInt);
          Assert.assertEquals(readInt, values[i]);
        }
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 2);
        mmapReader.close();
        // Assert.assertEquals(FileReaderTestUtils.getNumOpenFiles(file), 0);

        System.out.println("END MAX BITS:" + maxBits);
      } finally {
        file.delete();
      }

    }
  }
}
