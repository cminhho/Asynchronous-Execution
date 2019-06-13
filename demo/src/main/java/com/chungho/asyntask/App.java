package com.chungho.asyntask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/*
- folder 1:
  - file1
  - file2
  - file3
  - file1
  - file2
  # folder
  - file3
  - file1
  - file2
  - file3
  # folder
 */
public class App {

  private static int totalFiles = 20;
  // The number of items in a batch, when pushing directory contents asynchronously.
  private static final int itemBatchSize = 5;

  public static void main(String[] args) {
    int maxCrawlFileLimit = 5;

    ExecutorService asyncDirectoryPusherService = null;

    try {
      final String dir = "DocID_113132";
      List<String> batchOperations = new ArrayList<>();
      asyncDirectoryPusherService = Executors.newCachedThreadPool();

      for (int i = 0; i < totalFiles; i++) {
        batchOperations.add("Operation to index file with id: " + i);

        if (i >= maxCrawlFileLimit) {
          asyncDirectoryPusherService.submit(new AsyncDirectoryContentPusher(dir + "_" + i));
          break;
        }
      }

      postApiOperation(batchOperations);

    } finally {
      // shut down the executor manually
      asyncDirectoryPusherService.shutdown();
    }
  }

  private static void postApiOperation(List<String> batchOperations) {
    for (String operation : batchOperations) {
      System.out.println("[POST API] " + operation);
    }
  }

  private static class AsyncDirectoryContentPusher implements Runnable {

    private final String dir;

    public AsyncDirectoryContentPusher(String dir) {
      this.dir = dir;
    }

    public void run() {
      int count = 0;
      int batchIndex = 0;

      List<String> pushItems = new ArrayList<>();

      for (int i = 0; i < totalFiles; i++) {
        count++;
        pushItems.add("[AsyncTask] Operation to index file with id: " + i);
        if (count % itemBatchSize == 0) {
          batchIndex++;
          postApiOperationAsync(batchIndex, pushItems);
          pushItems = new ArrayList<>();
          count = 0;
        }
      }

      if (count > 0) {
        postApiOperationAsync(batchIndex, pushItems);
      }

    }

    private void postApiOperationAsync(int batch, List<String> pushItems) {
      for(String pushItem: pushItems){
        System.out.println("BATCH-"+batch+"[POST API ASYNC] " + pushItem);
      }
    }
  }
}
