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
  # folder
    - file1
    - file2
    - file3
  - file3
  - file1
  - file2
  - file3
  # folder
    - file2
  - file3
 */

/*
Reduce the total number of child-items for each operation down to: 1000

POST batch operations when reaching 1000 child-items

DO Background: when the folder has more 1000 child-items
	> create asynchronous execution to index remaining child-items concurrently in the background
		> add child items to pushItem
		> Send pushItem when reach the target of batch size
 */
public class App {

  private static int totalFolders = 10;
  private static int totalFilesInFolder = 10;
  // The number of items in a batch, when pushing directory contents asynchronously.
  private static final int itemBatchSize = 4;
  private static final int maxCrawlFileLimit = 2;
  private static final List<String> folders = new ArrayList<>();

  public static void main(String[] args) {
    configConnector();

    crawlFolders(folders);

    close();
  }

  private static void close() {
  }

  private static void configConnector() {
    getAllFoldersToCrawl();
    getAllTrashItemsToDelete();
  }

  private static void crawlFolders(List<String> folders) {
    ExecutorService asyncDirectoryPusherService = null;

    for(String folder: folders){
      System.out.println(""
          + "--------------------------------------------------------------------"+
          "\n--FOLDER: " + folder+
          "\n--------------------------------------------------------------------");
      try {
        List<String> batchOperations = new ArrayList<>();
        asyncDirectoryPusherService = Executors.newCachedThreadPool();

        for (int i = 1; i <= totalFilesInFolder; i++) {
          batchOperations.add(folder + " - Operation to index file with id: " + i);

          if (i >= maxCrawlFileLimit) {
            asyncDirectoryPusherService.submit(new AsyncPusher(folder));
            break;
          }
        }

        postApiOperation(batchOperations);

      } finally {
        // shut down the executor manually
        asyncDirectoryPusherService.shutdown();
      }
    }
  }

  private static void getAllTrashItemsToDelete() {
  }

  private static void getAllFoldersToCrawl() {
    for (int i = 1; i < totalFolders; i++) {
      folders.add("FOLDER_ID_" + i);
    }
  }

  private static void postApiOperation(List<String> batchOperations) {
    for (String operation : batchOperations) {
      System.out.println("[POST API] " + operation);
    }
  }

  private static class AsyncPusher implements Runnable {

    private final String folder;

    public AsyncPusher(String folder) {
      this.folder = folder;
    }

    public void run() {
      int count = 0;
      int batchIndex = 0;

      List<String> pushItems = new ArrayList<>();

      for (int i = maxCrawlFileLimit + 1; i <= totalFilesInFolder; i++) {
        count++;
        pushItems.add(folder + " - [AsyncTask] Operation to index file with id: " + i);
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
        System.out.println("[POST API ASYNC] BATCH-"+batch+ " : " + pushItem);
      }
    }
  }
}
