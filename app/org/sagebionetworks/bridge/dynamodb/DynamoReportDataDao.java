package org.sagebionetworks.bridge.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Resource;

import org.joda.time.LocalDate;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.BridgeUtils;
import org.sagebionetworks.bridge.dao.ReportDataDao;
import org.sagebionetworks.bridge.models.DateRangeResourceList;
import org.sagebionetworks.bridge.models.reports.ReportData;
import org.sagebionetworks.bridge.models.reports.ReportDataKey;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper.FailedBatch;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

@Component
public class DynamoReportDataDao implements ReportDataDao {

    private DynamoDBMapper mapper;

    @Resource(name = "reportDataMapper")
    final void setReportDataMapper(DynamoDBMapper reportDataMapper) {
        this.mapper = reportDataMapper;
    }
    
    @Override
    public DateRangeResourceList<? extends ReportData> getReportData(ReportDataKey key, LocalDate startDate, LocalDate endDate) {
        checkNotNull(key);
        checkNotNull(startDate);
        checkNotNull(endDate);
        
        DynamoReportData hashKey = new DynamoReportData();
        hashKey.setKey(key.getKeyString());
        
        // range key is between start date and end date
        Condition dateCondition = new Condition().withComparisonOperator(ComparisonOperator.BETWEEN)
                .withAttributeValueList(new AttributeValue().withS(startDate.toString()),
                        new AttributeValue().withS(endDate.toString()));

        DynamoDBQueryExpression<DynamoReportData> query =
                new DynamoDBQueryExpression<DynamoReportData>().withHashKeyValues(hashKey)
                        .withRangeKeyCondition("date", dateCondition);
        List<DynamoReportData> results = mapper.query(DynamoReportData.class, query);

        return new DateRangeResourceList<DynamoReportData>(results, startDate, endDate);
    }

    @Override
    public void saveReportData(ReportData reportData) {
        checkNotNull(reportData);
        
        mapper.save(reportData);
    }

    @Override
    public void deleteReportData(ReportDataKey key) {
        checkNotNull(key);

        DynamoReportData hashKey = new DynamoReportData();
        hashKey.setKey(key.getKeyString());

        DynamoDBQueryExpression<DynamoReportData> query =
                new DynamoDBQueryExpression<DynamoReportData>().withHashKeyValues(hashKey);
        List<DynamoReportData> objectsToDelete = mapper.query(DynamoReportData.class, query);
        
        if (!objectsToDelete.isEmpty()) {
            List<FailedBatch> failures = mapper.batchDelete(objectsToDelete);
            BridgeUtils.ifFailuresThrowException(failures);
        }
    }
    
    @Override
    public void deleteReportDataRecord(ReportDataKey key, LocalDate date) {
        checkNotNull(key);
        checkNotNull(date);

        DynamoReportData hashKey = new DynamoReportData();
        hashKey.setKey(key.getKeyString());
        hashKey.setDate(date);
        
        DynamoReportData reportDataRecord = mapper.load(hashKey);
        if (reportDataRecord != null) {
            mapper.delete(reportDataRecord);
        }
    }
}
