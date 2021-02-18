package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings.TransformSettings
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.NamedXContentRegistry

object TransformRunner : ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("TransformRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var esClient: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var transformMetadataService: TransformMetadataService
    private lateinit var transformSearchService: TransformSearchService

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Transform) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}]")
        }

        launch {
            try {
                val metadata = transformMetadataService.getMetadata(job)
                executeJob(job, metadata, context)
            } catch (e: Exception) {
                logger.error("Failed to fetch the transform metadata for job [${job.id}]")
                return@launch
            }
        }
    }

    suspend fun executeJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        if (shouldProcessTransform(transform, metadata)) {
            val lock = acquireLockForTransformJob(transform, context)
            if (lock == null) {
                logger.warn("Could not acquire lock for ${transform.id}")
            } else {

            }
        }
    }

    fun shouldProcessTransform(transform: Transform, metadata: TransformMetadata): Boolean {
        if (!transform.enabled ||
            listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FAILED, TransformMetadata.Status.FINISHED).contains(metadata.status)) {
            return false
        }

        return true
    }

    suspend fun acquireLockForTransformJob(transform: ScheduledJobParameter, context: JobExecutionContext): LockModel? {
        var lock: LockModel? = null
        try {
            BackoffPolicy.exponentialBackoff(
                TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
                TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
            ).retry(logger) {
                lock = context.lockService.suspendUntil { acquireLock(transform, context, it) }
            }
        } catch (e: Exception) {
            logger.error("")
        }

        return lock
    }
}
