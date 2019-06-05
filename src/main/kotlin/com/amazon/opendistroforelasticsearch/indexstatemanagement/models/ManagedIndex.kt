/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexstatemanagement.models

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.instant
import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.optionalTimeField
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.ScheduleParser
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.lucene.uid.Versions
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException
import java.time.Instant

// TODO: This is an unfinished ManagedIndex data class; just needed for the jobParser

data class ManagedIndex(
    val id: String = NO_ID,
    val version: Long = NO_VERSION,
    val jobName: String,
    val index: String,
    val enabled: Boolean,
    val jobSchedule: Schedule,
    val jobLastUpdatedTime: Instant,
    val jobEnabledTime: Instant?,
    val policyName: String,
    val policyVersion: Long?,
    val policy: Policy?,
    val changePolicy: ChangePolicy?
) : ScheduledJobParameter {

    init {
        if (enabled) {
            requireNotNull(jobEnabledTime)
        } else {
            require(jobEnabledTime == null)
        }
    }

    override fun isEnabled() = enabled

    override fun getName() = jobName

    override fun getEnabledTime() = jobEnabledTime

    override fun getSchedule() = jobSchedule

    override fun getLastUpdateTime() = jobLastUpdatedTime

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder
            .startObject()
                .startObject(MANAGED_INDEX_TYPE)
                    .field(NAME_FIELD, jobName)
                    .field(ENABLED_FIELD, enabled)
                    .field(INDEX_FIELD, index)
                    .field(SCHEDULE_FIELD, jobSchedule)
                    .optionalTimeField(LAST_UPDATED_TIME_FIELD, jobLastUpdatedTime)
                    .optionalTimeField(ENABLED_TIME_FIELD, jobEnabledTime)
                    .field(POLICY_NAME_FIELD, policyName)
                    .field(POLICY_VERSION_FIELD, policyVersion)
                    .field(POLICY_FIELD, policy, XCONTENT_WITHOUT_TYPE)
                    .field(CHANGE_POLICY_FIELD, changePolicy)
                .endObject()
            .endObject()
        return builder
    }

    companion object {
        const val MANAGED_INDEX_TYPE = "managed_index"
        const val NO_ID = ""
        const val NO_VERSION = Versions.NOT_FOUND
        const val NAME_FIELD = "name"
        const val ENABLED_FIELD = "enabled"
        const val SCHEDULE_FIELD = "schedule"
        const val LAST_UPDATED_TIME_FIELD = "last_updated_time"
        const val ENABLED_TIME_FIELD = "enabled_time"
        const val INDEX_FIELD = "index"
        const val POLICY_NAME_FIELD = "policy_name"
        const val POLICY_FIELD = "policy"
        const val POLICY_VERSION_FIELD = "policy_version"
        const val CHANGE_POLICY_FIELD = "change_policy"

        val logger = LogManager.getLogger(ManagedIndex::class.java)

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parse(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): ManagedIndex {
            lateinit var name: String
            lateinit var index: String
            lateinit var schedule: Schedule
            lateinit var policyName: String
            var policy: Policy? = null
            var changePolicy: ChangePolicy? = null
            var lastUpdatedTime: Instant? = null
            var enabledTime: Instant? = null
            var enabled = true
            var policyVersion = NO_VERSION

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    NAME_FIELD -> name = xcp.text()
                    INDEX_FIELD -> index = xcp.text()
                    ENABLED_FIELD -> enabled = xcp.booleanValue()
                    SCHEDULE_FIELD -> schedule = ScheduleParser.parse(xcp)
                    ENABLED_TIME_FIELD -> enabledTime = xcp.instant()
                    LAST_UPDATED_TIME_FIELD -> lastUpdatedTime = xcp.instant()
                    POLICY_NAME_FIELD -> policyName = xcp.text()
                    POLICY_VERSION_FIELD -> policyVersion = xcp.longValue()
                    POLICY_FIELD -> policy = if (xcp.currentToken() == Token.VALUE_NULL) null else Policy.parse(xcp)
                    CHANGE_POLICY_FIELD -> changePolicy = if (xcp.currentToken() == Token.VALUE_NULL) null else ChangePolicy.parse(xcp)
                    else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in ManagedIndex.")
                }
            }

            if (enabled && enabledTime == null) {
                enabledTime = Instant.now()
            } else if (!enabled) {
                enabledTime = null
            }

            return ManagedIndex(
                id,
                version,
                index = requireNotNull(index) { "ManagedIndex index is null" },
                jobName = requireNotNull(name) { "ManagedIndex name is null" },
                enabled = enabled,
                jobSchedule = requireNotNull(schedule) { "ManagedIndex schedule is null" },
                jobLastUpdatedTime = requireNotNull(lastUpdatedTime) { "ManagedIndex last updated time is null" },
                jobEnabledTime = enabledTime,
                policyName = requireNotNull(policyName) { "ManagedIndex policy name is null" },
                policyVersion = policyVersion,
                policy = policy,
                changePolicy = changePolicy
            )
        }

        @JvmStatic
        @JvmOverloads
        @Throws(IOException::class)
        fun parseWithType(xcp: XContentParser, id: String = NO_ID, version: Long = NO_VERSION): ManagedIndex {
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            val managedIndex = parse(xcp, id, version)
            ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            return managedIndex
        }
    }
}