package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.function.AnomalyTimeSeriesView;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRequest;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponse;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesRow;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class TimeSeriesUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesUtil.class);

  private TimeSeriesUtil() {
  }

  /**
   * Returns the time series that are needed by the given anomaly function for detecting anomalies.
   *
   * @param anomalyFunction the anomaly function for detecting anomalies
   * @param monitoringWindowStart inclusive
   * @param monitoringWindowEnd exclusive
   * @return the data that is needed by the anomaly function for detecting anomalies.
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public static TimeSeriesResponse getTimeSeriesResponseForAnomalyDetection(BaseAnomalyFunction anomalyFunction,
      long monitoringWindowStart, long monitoringWindowEnd)
      throws JobExecutionException, ExecutionException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunction.getSpec();

    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyFunction.getDataRangeIntervals(monitoringWindowStart, monitoringWindowEnd);

    String filterString = anomalyFunctionSpec.getFilters();
    Multimap<String, String> filters;
    if (StringUtils.isNotBlank(filterString)) {
      filters = ThirdEyeUtils.getFilterSet(filterString);
    } else {
      filters = HashMultimap.create();
    }

    List<String> groupByDimensions;
    String exploreDimensionString = anomalyFunctionSpec.getExploreDimensions();
    if (StringUtils.isNotBlank(exploreDimensionString)) {
      groupByDimensions = Arrays.asList(exploreDimensionString.trim().split(","));
    } else {
      groupByDimensions = Collections.emptyList();
    }

    TimeGranularity timeGranularity = new TimeGranularity(anomalyFunctionSpec.getBucketSize(),
        anomalyFunctionSpec.getBucketUnit());

    return getTimeSeriesResponseImpl(anomalyFunctionSpec, startEndTimeRanges, timeGranularity, filters, groupByDimensions,
        monitoringWindowStart, monitoringWindowEnd);
  }

  /**
   * Returns the time series that were used by the given anomaly function for detecting the anomaly.
   *
   * @param anomalyTimeSeriesView the time series view for presenting the current and baseline values for the anomaly
   * @param dimensionMap a dimension map that is used to construct the filter for retrieving the corresponding data
   *                     that was used to detected the anomaly
   * @param timeGranularity time granularity for the frontend
   * @param viewWindowStart inclusive
   * @param viewWindowEnd exclusive
   * @return the time series that were used by the given anomaly function for detecting the anomaly
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public static TimeSeriesResponse getTimeSeriesResponseForPresentation(AnomalyTimeSeriesView anomalyTimeSeriesView,
      DimensionMap dimensionMap, TimeGranularity timeGranularity, long viewWindowStart, long viewWindowEnd)
      throws JobExecutionException, ExecutionException {

    AnomalyFunctionDTO anomalyFunctionSpec = anomalyTimeSeriesView.getSpec();
    List<Pair<Long, Long>> startEndTimeRanges =
        anomalyTimeSeriesView.getDataRangeIntervals(viewWindowStart, viewWindowEnd);

    // Get the original filter
    Multimap<String, String> filters;
    String filterString = anomalyFunctionSpec.getFilters();
    if (StringUtils.isNotBlank(filterString)) {
      filters = ThirdEyeUtils.getFilterSet(filterString);
    } else {
      filters = HashMultimap.create();
    }

    // Decorate filters according to dimensionMap
    filters = ThirdEyeUtils.getFilterSetFromDimensionMap(dimensionMap, filters);

    boolean hasOTHERDimensionName = false;
    for (String dimensionValue : dimensionMap.values()) {
      if (!dimensionValue.equalsIgnoreCase("other")) {
        hasOTHERDimensionName = true;
        break;
      }
    }

    // groupByDimensions (i.e., exploreDimensions) is empty by default because the query for getting the time series
    // will have the decorated filters according to anomalies' explore dimensions.
    // However, if there exists any dimension with value "OTHER, then we need to honor the origin groupBy in order to
    // construct the data for OTHER
    List<String> groupByDimensions = Collections.emptyList();
    if (hasOTHERDimensionName && StringUtils.isNotBlank(anomalyFunctionSpec.getExploreDimensions().trim())) {
      groupByDimensions = Arrays.asList(anomalyFunctionSpec.getExploreDimensions().trim().split(","));
    }

    return getTimeSeriesResponseImpl(anomalyFunctionSpec, startEndTimeRanges, timeGranularity, filters,
        groupByDimensions, viewWindowStart, viewWindowEnd);
  }

  private static TimeSeriesResponse getTimeSeriesResponseImpl(AnomalyFunctionDTO anomalyFunctionSpec,
      List<Pair<Long, Long>> startEndTimeRanges, TimeGranularity timeGranularity, Multimap<String, String> filters,
      List<String> groupByDimensions, long monitoringWindowStart, long monitoringWindowEnd)
      throws JobExecutionException, ExecutionException {

    TimeSeriesHandler timeSeriesHandler =
        new TimeSeriesHandler(ThirdEyeCacheRegistry.getInstance().getQueryCache());

    // Seed request with top-level...
    TimeSeriesRequest request = new TimeSeriesRequest();
    request.setCollectionName(anomalyFunctionSpec.getCollection());
    List<MetricExpression> metricExpressions = Utils
        .convertToMetricExpressions(anomalyFunctionSpec.getMetric(),
            anomalyFunctionSpec.getMetricFunction(), anomalyFunctionSpec.getCollection());
    request.setMetricExpressions(metricExpressions);
    request.setAggregationTimeGranularity(timeGranularity);
    request.setEndDateInclusive(false);
    request.setFilterSet(filters);
    request.setGroupByDimensions(groupByDimensions);

    LOG.info("Found [{}] time ranges to fetch data", startEndTimeRanges.size());
    for (Pair<Long, Long> timeRange : startEndTimeRanges) {
      LOG.info("Start Time [{}], End Time [{}] for anomaly analysis", new DateTime(timeRange.getFirst()),
          new DateTime(timeRange.getSecond()));
    }

    Set<TimeSeriesRow> timeSeriesRowSet = new HashSet<>();
    // TODO : replace this with Pinot MultiQuery Request
    for (Pair<Long, Long> startEndInterval : startEndTimeRanges) {
      DateTime startTime = new DateTime(startEndInterval.getFirst());
      DateTime endTime = new DateTime(startEndInterval.getSecond());
      request.setStart(startTime);
      request.setEnd(endTime);

      LOG.info(
          "Fetching data with startTime: [{}], endTime: [{}], metricExpressions: [{}], timeGranularity: [{}]",
          startTime, endTime, metricExpressions, timeGranularity);

      try {
        LOG.debug("Executing {}", request);
        TimeSeriesResponse response = timeSeriesHandler.handle(request);
        timeSeriesRowSet.addAll(response.getRows());
      } catch (Exception e) {
        throw new JobExecutionException(e);
      }
    }
    List<TimeSeriesRow> timeSeriesRows = new ArrayList<>();
    timeSeriesRows.addAll(timeSeriesRowSet);

    return new TimeSeriesResponse(timeSeriesRows);
  }
}
