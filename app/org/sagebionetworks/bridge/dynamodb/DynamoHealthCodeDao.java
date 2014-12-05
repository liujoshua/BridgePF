package org.sagebionetworks.bridge.dynamodb;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.sagebionetworks.bridge.dao.HealthCodeDao;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

public class DynamoHealthCodeDao implements HealthCodeDao {

    private DynamoDBMapper mapper;

    public void setDynamoDbClient(AmazonDynamoDB client) {
        DynamoDBMapperConfig mapperConfig = new DynamoDBMapperConfig.Builder().withSaveBehavior(SaveBehavior.UPDATE)
                .withConsistentReads(ConsistentReads.CONSISTENT)
                .withTableNameOverride(TableNameOverrideFactory.getTableNameOverride(DynamoHealthCode.class)).build();
        mapper = new DynamoDBMapper(client, mapperConfig);
    }

    @Override
    public boolean setIfNotExist(String code) {
        checkArgument(isNotBlank(code));
        try {
            DynamoHealthCode toSave = new DynamoHealthCode(code);
            mapper.save(toSave);
            return true;
        } catch(ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public void deleteCode(String healthCode) {
        checkNotNull(healthCode);
        DynamoHealthCode code = new DynamoHealthCode(healthCode);
        DynamoHealthCode dynamoHealthCode = mapper.load(code);
        if (dynamoHealthCode != null) {
            mapper.delete(dynamoHealthCode);
        }
    }
}
