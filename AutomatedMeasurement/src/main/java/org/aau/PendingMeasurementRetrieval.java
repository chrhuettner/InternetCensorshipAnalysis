package org.aau;

import me.tongfei.progressbar.ProgressBar;

import java.util.ArrayList;
import java.util.List;

import static org.aau.RipeAtlasMeasurement.waitForResultAndSaveToDb;

public class PendingMeasurementRetrieval {

    public static void main(String[] args) {
        List<Long> ids = SQLiteConnector.getPendingMeasurements();

        try (ProgressBar pb = new ProgressBar("Waiting for pending measurements (this can take some time)", ids.size())) {
            List<Thread> threads = new ArrayList<>();
            for (Long measurementId : ids) {
                Thread thread = Thread.startVirtualThread(() -> {
                    try {
                        waitForResultAndSaveToDb(measurementId, Integer.MAX_VALUE, 10000, pb);
                        SQLiteConnector.deletePendingMeasurement(measurementId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                threads.add(thread);
            }

            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
