package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFeedbackBean;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.datalayer.pojo.RawAnomalyResultBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.EmailConfigurationBean;
import com.linkedin.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class MergedAnomalyResultManagerImpl extends AbstractManagerImpl<MergedAnomalyResultDTO>
    implements MergedAnomalyResultManager {
  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME =
      " where collection=:collection and metric=:metric " + "and dimensions in (:dimensions) "
          + "and (startTime < :endTime and endTime > :startTime) " + "order by endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_METRIC_TIME =
      "where collection=:collection and metric=:metric "
          + "and (startTime < :endTime and endTime > :startTime) order by endTime desc";

  // find a conflicting window
  private static final String FIND_BY_COLLECTION_TIME = "where collection=:collection "
      + "and (startTime < :endTime and endTime > :startTime) order by endTime desc";

  private static final String FIND_BY_FUNCTION_ID = "where functionId=:functionId";

  private static final String FIND_BY_FUNCTION_AND_DIMENSIONS =
      "where functionId=:functionId " + "and dimensions=:dimensions order by endTime desc limit 1";

  private static final String FIND_BY_FUNCTION_AND_NULL_DIMENSION =
      "where functionId=:functionId " + "and dimensions is null order by endTime desc";

  private ExecutorService executorService = Executors.newFixedThreadPool(10);

  public MergedAnomalyResultManagerImpl() {
    super(MergedAnomalyResultDTO.class, MergedAnomalyResultBean.class);
  }

  public Long save(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(mergedAnomalyResultDTO);
      return mergedAnomalyResultDTO.getId();
    }
    MergedAnomalyResultBean mergeAnomalyBean = convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
    Long id = genericPojoDao.put(mergeAnomalyBean);
    mergedAnomalyResultDTO.setId(id);
    return id;
  }

  public int update(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
    if (mergedAnomalyResultDTO.getId() == null) {
      Long id = save(mergedAnomalyResultDTO);
      if (id > 0) {
        return 1;
      } else {
        return 0;
      }
    } else {
      MergedAnomalyResultBean mergeAnomalyBean =
          convertMergeAnomalyDTO2Bean(mergedAnomalyResultDTO);
      return genericPojoDao.update(mergeAnomalyBean);
    }
  }

  public MergedAnomalyResultDTO findById(Long id) {
    MergedAnomalyResultBean mergedAnomalyResultBean =
        genericPojoDao.get(id, MergedAnomalyResultBean.class);
    if (mergedAnomalyResultBean != null) {
      MergedAnomalyResultDTO mergedAnomalyResultDTO;
      mergedAnomalyResultDTO = convertMergedAnomalyBean2DTO(mergedAnomalyResultBean);
      return mergedAnomalyResultDTO;
    } else {
      return null;
    }
  }

  @Override
  public List<MergedAnomalyResultDTO> getAllByTimeEmailIdAndNotifiedFalse(long startTime,
      long endTime, long emailConfigId) {
    EmailConfigurationBean emailConfigurationBean =
        genericPojoDao.get(emailConfigId, EmailConfigurationBean.class);
    List<Long> functionIds = emailConfigurationBean.getFunctionIds();
    if (functionIds == null || functionIds.isEmpty()) {
      return Collections.emptyList();
    }
    Long[] functionIdArray = functionIds.toArray(new Long[] {});
    Predicate predicate = Predicate.AND(//
        Predicate.LT("startTime", endTime), //
        Predicate.GT("endTime", startTime), //
        Predicate.IN("functionId", functionIdArray), //
        Predicate.EQ("notified", false)//
    );
    List<MergedAnomalyResultBean> list =
        genericPojoDao.get(predicate, MergedAnomalyResultBean.class);
    return batchConvertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByFunctionId(Long functionId) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(FIND_BY_FUNCTION_ID,
        filterParams, MergedAnomalyResultBean.class);
    return batchConvertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricDimensionsTime(String collection,
      String metric, String dimensions, long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("dimensions", dimensions);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_DIMENSIONS_TIME, filterParams, MergedAnomalyResultBean.class);
    return batchConvertMergedAnomalyBean2DTO(list);
  }


  @Override
  public List<MergedAnomalyResultDTO> findByCollectionMetricTime(String collection, String metric,
      long startTime, long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("metric", metric);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_METRIC_TIME, filterParams, MergedAnomalyResultBean.class);
    return batchConvertMergedAnomalyBean2DTO(list);
  }

  @Override
  public List<MergedAnomalyResultDTO> findByCollectionTime(String collection, long startTime,
      long endTime) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("collection", collection);
    filterParams.put("startTime", startTime);
    filterParams.put("endTime", endTime);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_COLLECTION_TIME, filterParams, MergedAnomalyResultBean.class);
    return batchConvertMergedAnomalyBean2DTO(list);
  }


  @Override
  public MergedAnomalyResultDTO findLatestByFunctionIdDimensions(Long functionId,
      String dimensions) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);
    filterParams.put("dimensions", dimensions);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_FUNCTION_AND_DIMENSIONS, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = batchConvertMergedAnomalyBean2DTO(list);
    // TODO: Check list size instead of result size?
    if (result.size() > 0) {
      return result.get(0);
    }
    return null;
  }


  @Override
  public MergedAnomalyResultDTO findLatestByFunctionIdOnly(Long functionId) {
    Map<String, Object> filterParams = new HashMap<>();
    filterParams.put("functionId", functionId);

    List<MergedAnomalyResultBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_FUNCTION_AND_NULL_DIMENSION, filterParams, MergedAnomalyResultBean.class);
    List<MergedAnomalyResultDTO> result = batchConvertMergedAnomalyBean2DTO(list);
    // TODO: Check list size instead of result size?
    if (result.size() > 0) {
      return result.get(0);
    }
    return null;
  }


  protected MergedAnomalyResultBean convertMergeAnomalyDTO2Bean(MergedAnomalyResultDTO entity) {
    MergedAnomalyResultBean bean =
        (MergedAnomalyResultBean) convertDTO2Bean(entity, MergedAnomalyResultBean.class);
    if (entity.getFeedback() != null) {
      if (entity.getFeedback().getId() == null) {
        AnomalyFeedbackBean feedbackBean =
            (AnomalyFeedbackBean) convertDTO2Bean(entity.getFeedback(), AnomalyFeedbackBean.class);
        Long feedbackId = genericPojoDao.put(feedbackBean);
        entity.getFeedback().setId(feedbackId);
      }
      bean.setAnomalyFeedbackId(entity.getFeedback().getId());
    }
    if (entity.getFunction() != null) {
      bean.setFunctionId(entity.getFunction().getId());
    }

    if (entity.getAnomalyResults() != null && !entity.getAnomalyResults().isEmpty()) {
      List<Long> rawAnomalyIds = new ArrayList<>();
      for (RawAnomalyResultDTO rawAnomalyDTO : entity.getAnomalyResults()) {
        rawAnomalyIds.add(rawAnomalyDTO.getId());
      }
      bean.setRawAnomalyIdList(rawAnomalyIds);
    }
    return bean;
  }

  protected MergedAnomalyResultDTO convertMergedAnomalyBean2DTO(
      MergedAnomalyResultBean mergedAnomalyResultBean) {
    MergedAnomalyResultDTO mergedAnomalyResultDTO;
    mergedAnomalyResultDTO =
        MODEL_MAPPER.map(mergedAnomalyResultBean, MergedAnomalyResultDTO.class);
    if (mergedAnomalyResultBean.getFunctionId() != null) {
      AnomalyFunctionBean anomalyFunctionBean =
          genericPojoDao.get(mergedAnomalyResultBean.getFunctionId(), AnomalyFunctionBean.class);
      AnomalyFunctionDTO anomalyFunctionDTO =
          MODEL_MAPPER.map(anomalyFunctionBean, AnomalyFunctionDTO.class);
      mergedAnomalyResultDTO.setFunction(anomalyFunctionDTO);
    }
    if (mergedAnomalyResultBean.getAnomalyFeedbackId() != null) {
      AnomalyFeedbackBean anomalyFeedbackBean = genericPojoDao
          .get(mergedAnomalyResultBean.getAnomalyFeedbackId(), AnomalyFeedbackBean.class);
      AnomalyFeedbackDTO anomalyFeedbackDTO =
          MODEL_MAPPER.map(anomalyFeedbackBean, AnomalyFeedbackDTO.class);
      mergedAnomalyResultDTO.setFeedback(anomalyFeedbackDTO);
    }
    if (mergedAnomalyResultBean.getRawAnomalyIdList() != null
        && !mergedAnomalyResultBean.getRawAnomalyIdList().isEmpty()) {
      List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();
      List<RawAnomalyResultBean> list = genericPojoDao
          .get(mergedAnomalyResultBean.getRawAnomalyIdList(), RawAnomalyResultBean.class);
      for (RawAnomalyResultBean rawAnomalyResultBean : list) {
        anomalyResults.add(createRawAnomalyDTOFromBean(rawAnomalyResultBean));
      }
      mergedAnomalyResultDTO.setAnomalyResults(anomalyResults);
    }

    return mergedAnomalyResultDTO;
  }

  protected List<MergedAnomalyResultDTO> batchConvertMergedAnomalyBean2DTO(
      List<MergedAnomalyResultBean> mergedAnomalyResultBeanList) {
    List<Future<MergedAnomalyResultDTO>> mergedAnomalyResultDTOFutureList = new ArrayList<>(mergedAnomalyResultBeanList.size());
    for (MergedAnomalyResultBean mergedAnomalyResultBean : mergedAnomalyResultBeanList) {
      Future<MergedAnomalyResultDTO> future =
          executorService.submit(() -> convertMergedAnomalyBean2DTO(mergedAnomalyResultBean));
      mergedAnomalyResultDTOFutureList.add(future);
    }

    List<MergedAnomalyResultDTO> mergedAnomalyResultDTOList = new ArrayList<>(mergedAnomalyResultBeanList.size());
    for (Future future : mergedAnomalyResultDTOFutureList) {
      try {
        mergedAnomalyResultDTOList.add((MergedAnomalyResultDTO) future.get(60, TimeUnit.SECONDS));
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        LOG.warn("Failed to convert MergedAnomalyResultDTO from bean: {}", e.toString());
      }
    }

    return mergedAnomalyResultDTOList;
  }
}
