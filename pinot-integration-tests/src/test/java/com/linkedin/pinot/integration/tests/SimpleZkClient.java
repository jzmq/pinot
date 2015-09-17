package com.linkedin.pinot.integration.tests;

/**
 * Created by dengqiang on 9/17/15.
 */

import org.apache.zookeeper.*;

import java.io.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * User: Johnny
 * Date: 11/19/14
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleZkClient implements Watcher {


  private ZooKeeper zooKeeper = null;
  private static CountDownLatch countDownLatch = new CountDownLatch(1);

  String CONNECT_STRING = "10.10.2.129:2181";
  private static final int SESSION_TIMEOUT = 10000;

  public SimpleZkClient(String conStr) {
    super();
    CONNECT_STRING = conStr;
  }

  public static void waitUntilConnected(ZooKeeper zooKeeper, CountDownLatch connectedLatch) {
    if (ZooKeeper.States.CONNECTING == zooKeeper.getState()) {
      try {
        connectedLatch.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public void createConnection() {
    try {
      zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, this);
      //countDownLatch.await();
      waitUntilConnected(zooKeeper, countDownLatch);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  public void releaseConnection() {

    if (this.zooKeeper != null) {
      try {
        this.zooKeeper.close();
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  public void createPath(String path, byte[] data) throws KeeperException, InterruptedException {
    zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  public void writeToPath(String path) {
  }

  public void deletePath(String path) throws KeeperException, InterruptedException {
      List<String> childList = zooKeeper.getChildren(path, this);
      if (childList.isEmpty()) {
        zooKeeper.delete(path, -1);
        System.out.println("Delete path:" + path);
      } else {
        for (String childName : childList) {
          // TODO 增加filter
          String childPath = path + "/" + childName;
          deletePath(childPath);
        }
        deletePath(path);
      }
  }

  public byte[] readZnode(String path) throws KeeperException, InterruptedException {
    return zooKeeper.getData(path, false, zooKeeper.exists(path, true));
  }

  public void saveNodeToLocal(String path, File localFile) throws KeeperException, InterruptedException, IOException {
    byte[] res = readZnode(path);
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(localFile));
    bufferedOutputStream.write(res);
    bufferedOutputStream.flush();
    bufferedOutputStream.close();
  }

  public void createNodeFromLocal(String path, File file) throws IOException, KeeperException, InterruptedException {
    byte[] data = new byte[(int) file.length()];
    BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
    bufferedInputStream.read(data, 0, (int) file.length());
    createPath(path, data);
    bufferedInputStream.close();
  }

//  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
////        String localPath = "/PinotCluster/IDEALSTATES/flights_OFFLINE";
////        File localFile = new File("/Users/Johnny/code/Sample/src/main/java/Zookeeper/DataFile");
//    File file = new File("/Users/Johnny/code/Sample/src/main/java/Zookeeper/audienx_OFFLINE_IDEALSTATES");
//    String path = "/PinotCluster/IDEALSTATES/audienx_OFFLINE";
//    File file2 = new File("/Users/Johnny/code/Sample/src/main/java/Zookeeper/audienx_OFFLINE_EXTERNALVIEW");
//    String path2 = "/PinotCluster/EXTERNALVIEW/audienx_OFFLINE";
//
//    Main main = new Main();
//    main.createConnection();
//
//    String p = "/PinotCluster/PROPERTYSTORE/SEGMENTS/audienx_OFFLINE";
//
//    main.deletePath(p);
//
////        main.saveNodeToLocal(localPath, localFile);
////        main.saveNodeToLocal(path, file);
////        main.saveNodeToLocal(path2, file2);
////        main.createNodeFromLocal(path,file);
////        main.createNodeFromLocal(path2,file2);
////        main.createNodeFromLocal(localPath,localFile);
//    main.releaseConnection();
//    System.out.println("ok");
//  }

  public void process(WatchedEvent watchedEvent) {
    //To change body of implemented methods use File | Settings | File Templates.

    if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
      countDownLatch.countDown();
    }
  }
}