package com.linkedin.thirdeye.anomaly.api.task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.api.function.exception.FunctionDidNotEvaluateException;
import com.linkedin.thirdeye.anomaly.util.ThirdEyeRequestUtils;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 * This class represents the data access to the third eye server and anomaly database.
 */
public abstract class AbstractBaseAnomalyDetectionTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBaseAnomalyDetectionTask.class);

  private final AnomalyDetectionTaskInfo taskInfo;
  private final StarTreeConfig starTreeConfig;
  private final AnomalyDetectionFunction function;
  private final AnomalyDetectionFunctionHistory functionHistory;
  private final ThirdEyeClient thirdEyeClient;

  /** the metric specs that the driver queries the third-eye server for */
  private final List<String> metricsRequiredByTask;

  /** the time range of data that the driver needs to provide to the function */
  private final TimeRange queryTimeRange;

  /** the time range to monitor for anomalies */
  private final TimeRange monitoringWindow;

  public AbstractBaseAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient) {
    this(starTreeConfig, taskInfo, function, functionHistory, thirdEyeClient, new HashSet<String>());
  }

  /**
   * @param starTreeConfig
   * @param taskInfo
   * @param function
   * @param functionHistory
   * @param thirdEyeClient
   */
  public AbstractBaseAnomalyDetectionTask(
      StarTreeConfig starTreeConfig,
      AnomalyDetectionTaskInfo taskInfo,
      AnomalyDetectionFunction function,
      AnomalyDetectionFunctionHistory functionHistory,
      ThirdEyeClient thirdEyeClient,
      Set<String> additionalMetricsRequiredByTask)
  {
    this.starTreeConfig = starTreeConfig;
    this.taskInfo = taskInfo;
    this.function = function;
    this.functionHistory = functionHistory;
    this.thirdEyeClient = thirdEyeClient;

    Set<String> metricNames = new HashSet<>();
    metricNames.addAll(function.getMetrics());
    metricNames.addAll(additionalMetricsRequiredByTask);

    metricsRequiredByTask = new ArrayList<>(metricNames);

    monitoringWindow = getMonitoringWindow(taskInfo.getTimeRange(),
        function.getMinimumMonitoringIntervalTimeGranularity());

    queryTimeRange = getTaskTimeSeriesRange(monitoringWindow, function);

    // initialize function history
    functionHistory.init(queryTimeRange);
  }

  /**
   * @param dimensionKey
   * @param metricTimeSeries
   * @return
   *  The list of anomalies produced by the function
   * @throws FunctionDidNotEvaluateException
   */
  protected List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries metricTimeSeries)
      throws FunctionDidNotEvaluateException {
    List<AnomalyResult> anomalyResults = function.analyze(dimensionKey, metricTimeSeries, monitoringWindow,
        functionHistory.getHistoryForDimensionKey(dimensionKey));
    filterAnomalyResults(anomalyResults, monitoringWindow);
    return anomalyResults;
  }

  /**
   * @return
   *  The DimensionKeys and MetricTimeSeries from a query
   * @throws Exception
   */
  protected Map<DimensionKey, MetricTimeSeries> getDataset(Map<String, String> fixedDimensionValues,
      String groupByDimension) throws Exception {
    ThirdEyeRequest request = ThirdEyeRequestUtils.buildRequest(starTreeConfig.getCollection(),
        groupByDimension, fixedDimensionValues, metricsRequiredByTask, function.getAggregationTimeGranularity(),
        queryTimeRange);
    Map<DimensionKey, MetricTimeSeries> dataset = thirdEyeClient.execute(request);

    /*
     * Verify that the dataset returned the values we expected.
     */
    sanityCheckDataset(dataset);

    return dataset;
  }

  protected AnomalyDetectionTaskInfo getTaskInfo() {
    return taskInfo;
  }

  protected StarTreeConfig getStarTreeConfig() {
    return starTreeConfig;
  }

  protected AnomalyDetectionFunction getFunction() {
    return function;
  }

  /*
   * Private helper methods
   */

  /**
   * Perform basic sanity check on the dataset and log warnings if failed.
   *
   * @param dataset
   */
  private void sanityCheckDataset(Map<DimensionKey, MetricTimeSeries> dataset) {
    for (DimensionKey dimensionKey : dataset.keySet()) {
      MetricTimeSeries metricTimeSeries = dataset.get(dimensionKey);
      Set<Long> seriesTimeWindowSet = metricTimeSeries.getTimeWindowSet();
      if (seriesTimeWindowSet.contains(monitoringWindow.getStart()) == false) {
        LOGGER.warn("dataset series {} does not contain expected start time window {}", dimensionKey,
            monitoringWindow.getStart());
      }
      long lastExpectedTimeWindow =
          monitoringWindow.getEnd() - TimeGranularityUtils.toMillis(function.getAggregationTimeGranularity());
      if (lastExpectedTimeWindow != monitoringWindow.getStart() &&
          seriesTimeWindowSet.contains(lastExpectedTimeWindow) == false) {
        LOGGER.warn("dataset series {} does not contain expected end time window {}", dimensionKey,
            lastExpectedTimeWindow);
      }
    }
  }

  /**
   * @param monitoringInterval
   * @param function
   * @return
   *  The range of data required for the task, determined by the start and end times defined in the taskInfo and the
   *  requirements of the function.
   */
  private static TimeRange getTaskTimeSeriesRange(TimeRange monitoringInterval, AnomalyDetectionFunction function)
  {
    long end = monitoringInterval.getEnd();
    TimeGranularity functionGranularity = function.getTrainingWindowTimeGranularity();
    long start;
    if (functionGranularity != null) {
      // compute the start time of the dataset
      start = monitoringInterval.getStart() - TimeGranularityUtils.toMillis(functionGranularity)
          - TimeGranularityUtils.toMillis(function.getAggregationTimeGranularity());
    } else {
      start = 0; // all time
    }
    return new TimeRange(start, end);
  }

  /**
   * @param anomalyResults
   *  The list of anomalyResults to be filtered
   */
  private static void filterAnomalyResults(List<AnomalyResult> anomalyResults, TimeRange timeRange) {
    Iterator<AnomalyResult> it = anomalyResults.iterator();
    while (it.hasNext()) {
      AnomalyResult anomalyResult = it.next();

      boolean remove = false;

      if (anomalyResult.isAnomaly() == false) {
        remove = true;
      }

      if (timeRange.contains(anomalyResult.getTimeWindow()) == false) {
        LOGGER.debug("function produced anomaly result not in window {}", timeRange);
        remove = true;
      }

      if (remove) {
        it.remove();
      }
    }
  }

  /**
   * @param monitoringInterval
   * @param minMonitoringTimeGranularity
   * @return
   */
  private static TimeRange getMonitoringWindow(TimeRange monitoringInterval,
      TimeGranularity minMonitoringTimeGranularity) {
    if (minMonitoringTimeGranularity == null) {
      return monitoringInterval;
    }

    long minMonitoringWindowMillis = TimeGranularityUtils.toMillis(minMonitoringTimeGranularity);
    long monitoringIntervalMillis = monitoringInterval.getEnd() - monitoringInterval.getStart();
    if (minMonitoringWindowMillis > monitoringIntervalMillis) {
      return new TimeRange(monitoringInterval.getEnd() - minMonitoringWindowMillis, monitoringInterval.getEnd());
    } else {
      return monitoringInterval;
    }
  }

}
