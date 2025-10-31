package broker

import (
	"log"

    "github.com/mcheviron/elise/internal/metadata"
)

const topicAuthorizedOpsUnknown = int32(-2147483648)

func (s *Server) buildDescribeTopicPartitionsResponse(req *DescribeTopicPartitionsRequest) DescribeTopicPartitionsResponse {
	cluster := s.metaManager.Cluster()
	if cluster == nil {
		log.Printf("metadata manager returned nil cluster")
	}

	topics := make([]DescribeTopicPartitionsResponseTopic, 0, len(req.Topics))
	for _, topic := range req.Topics {
		topics = append(topics, buildTopicResponse(cluster, topic))
	}

	return DescribeTopicPartitionsResponse{
		ThrottleTimeMS: 0,
		Topics:         topics,
		NextCursor:     nil,
		TaggedFields:   nil,
	}
}

func buildTopicResponse(cluster *metadata.Cluster, topicReq DescribeTopicPartitionsRequestTopic) DescribeTopicPartitionsResponseTopic {
	response := DescribeTopicPartitionsResponseTopic{
		ErrorCode:                 ErrorCodeUnknownTopicOrPartition,
		Name:                      nil,
		TopicID:                   [16]byte{},
		IsInternal:                false,
		Partitions:                nil,
		NextCursor:                nil,
		TopicAuthorizedOperations: topicAuthorizedOpsUnknown,
		TaggedFields:              nil,
	}

	if cluster == nil {
		if topicReq.Name != "" {
			name := topicReq.Name
			response.Name = &name
		}
		return response
	}

	var (
		metaTopic *metadata.Topic
		ok        bool
	)

	if topicReq.Name != "" {
		metaTopic, ok = cluster.TopicByName(topicReq.Name)
	}
	if !ok && topicReq.TopicID != ([16]byte{}) {
		metaTopic, ok = cluster.TopicByID(topicReq.TopicID)
	}

	if !ok || metaTopic == nil {
		if topicReq.Name != "" {
			name := topicReq.Name
			response.Name = &name
		}
		return response
	}

	name := metaTopic.Name
	if name == "" {
		name = topicReq.Name
	}
	response.Name = &name
	response.TopicID = metaTopic.ID
	response.ErrorCode = ErrorCodeNone
	response.Partitions = buildPartitionResponses(topicReq, metaTopic)

	return response
}

func buildPartitionResponses(topicReq DescribeTopicPartitionsRequestTopic, metaTopic *metadata.Topic) []DescribeTopicPartitionsResponsePartition {
	if metaTopic == nil {
		return nil
	}

	if len(topicReq.Partitions) == 0 {
		metaPartitions := metaTopic.PartitionList()
		partitions := make([]DescribeTopicPartitionsResponsePartition, 0, len(metaPartitions))
		for _, p := range metaPartitions {
			partitions = append(partitions, buildPartitionResponse(p))
		}
		return partitions
	}

	partitions := make([]DescribeTopicPartitionsResponsePartition, 0, len(topicReq.Partitions))
	for _, requested := range topicReq.Partitions {
		if part, ok := metaTopic.Partitions[requested.PartitionIndex]; ok && part != nil {
			partitions = append(partitions, buildPartitionResponse(part))
			continue
		}

		partitions = append(partitions, DescribeTopicPartitionsResponsePartition{
			ErrorCode:              ErrorCodeUnknownTopicOrPartition,
			PartitionIndex:         requested.PartitionIndex,
			LeaderID:               -1,
			LeaderEpoch:            -1,
			ReplicaNodes:           []int32{},
			IsrNodes:               []int32{},
			EligibleLeaderReplicas: nil,
			LastKnownElr:           nil,
			OfflineReplicas:        []int32{},
			TaggedFields:           nil,
		})
	}

	return partitions
}

func buildPartitionResponse(metadataPartition *metadata.Partition) DescribeTopicPartitionsResponsePartition {
	return DescribeTopicPartitionsResponsePartition{
		ErrorCode:              ErrorCodeNone,
		PartitionIndex:         metadataPartition.PartitionID,
		LeaderID:               metadataPartition.LeaderID,
		LeaderEpoch:            metadataPartition.LeaderEpoch,
		ReplicaNodes:           cloneRequiredInt32Slice(metadataPartition.Replicas),
		IsrNodes:               cloneRequiredInt32Slice(metadataPartition.ISR),
		EligibleLeaderReplicas: cloneOptionalInt32Slice(metadataPartition.EligibleLeaderReplicas),
		LastKnownElr:           cloneOptionalInt32Slice(metadataPartition.LastKnownEligibleLeaders),
		OfflineReplicas:        cloneRequiredInt32Slice(metadataPartition.OfflineReplicas),
		TaggedFields:           nil,
	}
}

func cloneRequiredInt32Slice(src []int32) []int32 {
	if len(src) == 0 {
		return []int32{}
	}
	dst := make([]int32, len(src))
	copy(dst, src)
	return dst
}

func cloneOptionalInt32Slice(src []int32) []int32 {
	if src == nil {
		return nil
	}
	return cloneRequiredInt32Slice(src)
}
