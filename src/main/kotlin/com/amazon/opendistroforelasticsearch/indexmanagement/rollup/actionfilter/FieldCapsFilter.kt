package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.actionfilter

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getRollupJobs
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.populateFieldMappings
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.fieldcaps.FieldCapabilities
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.action.support.ActionFilterChain
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.xcontent.DeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.tasks.Task

private val logger = LogManager.getLogger(FieldCapsFilter::class.java)

@Suppress("UNCHECKED_CAST")
class FieldCapsFilter(
    val clusterService: ClusterService,
    val indexNameExpressionResolver: IndexNameExpressionResolver
) : ActionFilter {

    override fun <Request : ActionRequest?, Response : ActionResponse?> apply(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
        chain: ActionFilterChain<Request, Response>
    ) {
        if (request is FieldCapabilitiesRequest) {
            chain.proceed(task, action, request, object : ActionListener<Response> {
                override fun onResponse(response: Response) {
                    val indices = request.indices().map { it.toString() }.toTypedArray()
                    val concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
                    val rollupIndices = mutableSetOf<String>()
                    for (index in concreteIndices) {
                        val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)
                        if (isRollupIndex) {
                            rollupIndices.add(index)
                        }
                    }

                    if (rollupIndices.size > 0) {
                        logger.info("Need to rewrite field mapping response")
                        response as FieldCapabilitiesResponse
                        val rollupFieldsIndexMap = getSourceMappingsForRollupJob(rollupIndices)
                        val fieldsToRemove = mutableSetOf<String>()
                        rollupFieldsIndexMap.keys.forEach {
                            fieldsToRemove.addAll(it.internalFieldMappings())
                        }
                        fieldsToRemove.addAll(mutableSetOf("rollup", "rollup._id", "rollup._schema_version"))
                        val fieldsToUpdate = populateFieldsToUpdate(rollupFieldsIndexMap)
                        val rewritten = rewriteFields(response.get(), response.indices, fieldsToRemove, fieldsToUpdate)

                        val builder = XContentFactory.jsonBuilder().prettyPrint()
                        builder.startObject()
                        builder.field("indices", response.indices)
                        builder.field("fields", rewritten as Map<String, Any>?)
                        builder.endObject()

                        val parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler
                                .THROW_UNSUPPORTED_OPERATION, Strings.toString(builder))
                        val reWrittenResponse = FieldCapabilitiesResponse.fromXContent(parser)
                        listener.onResponse(reWrittenResponse as Response)
                    } else {
                        listener.onResponse(response)
                    }
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            })
        } else {
            chain.proceed(task, action, request, listener)
        }

    }

    private fun populateFieldsToUpdate(fieldMappingIndexMap: Map<RollupFieldMapping, Set<String>>): Map<String, Pair<Set<String>, Set<String>>> {
        val result = mutableMapOf<String, Pair<MutableSet<String>, MutableSet<String>>>()
        fieldMappingIndexMap.keys.forEach {
            if (it.sourceType != null) {
                if (result[it.fieldName] == null) {
                    result[it.fieldName] = mutableSetOf<String>() to mutableSetOf()
                }
                val (types, indices) = result.getValue(it.fieldName)
                types.add(it.sourceType!!)
                indices.addAll(fieldMappingIndexMap.getValue(it))
            }
        }

        return result
    }

    private fun populateRollupIndexFieldMappings(rollupIndex: String): Map<String, Set<RollupFieldMapping>> {
        clusterService.state().metadata.index(rollupIndex)
        val fieldMappings = mutableMapOf<String, MutableSet<RollupFieldMapping>>()
        val rollupJobs = clusterService.state().metadata.index(rollupIndex).getRollupJobs() ?: return fieldMappings
        rollupJobs.forEach {rollup ->
            if (fieldMappings[rollup.targetIndex] == null) {
                fieldMappings[rollup.targetIndex] = mutableSetOf()
            }
            fieldMappings[rollup.targetIndex]!!.addAll(populateRollupJobFieldMappings(rollup))
        }
        return fieldMappings
    }

    private fun populateRollupJobFieldMappings(rollup: Rollup): Set<RollupFieldMapping> {
        val rollupFieldMappings = rollup.populateFieldMappings()
        val sourceIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), rollup.sourceIndex)
        sourceIndices.forEach {
            val mappings = clusterService.state().metadata.index(it).mapping()?.sourceAsMap ?: return rollupFieldMappings
            rollupFieldMappings.forEach { fieldMapping ->
                val fieldType = getFieldType(fieldMapping.fieldName, mappings)
                if (fieldType != null) {
                    fieldMapping.sourceType(fieldType)
                }
            }
        }

        return rollupFieldMappings
    }

    private fun rewriteFields(
        fields: Map<String, Map<String, FieldCapabilities>>,
        indices: Array<String>,
        fieldsToRemove: Set<String>,
        fieldsToUpdate: Map<String, Pair<Set<String>, Set<String>>>
    ): MutableMap<String, Map<String, FieldCapabilities>> {
        val top = mutableMapOf<String, Map<String, FieldCapabilities>>()
        fields.keys.forEach {
            if (!fieldsToRemove.contains(it) && fields[it] != null) {
                // Not sure
                if (fieldsToUpdate.keys.contains(it)) {
                    val rewrittenType = mutableMapOf<String, FieldCapabilities>()
                    val types = fields[it]
                    val existingTypes = types?.keys
                    val typeToReWrite = fieldsToUpdate.getValue(it).first.elementAt(0)
                    if (existingTypes != null && existingTypes.contains(typeToReWrite)) {
                        val fieldCaps = types.getValue(typeToReWrite)
                        val updatedIndices = mutableSetOf<String>()
                        updatedIndices.addAll(fieldsToUpdate.getValue(it).second)
                        if (fieldCaps.indices() == null) {
                            updatedIndices.addAll(indices)
                        } else {
                            updatedIndices.addAll(fieldCaps.indices())
                        }
                        rewrittenType[typeToReWrite] = FieldCapabilities(fieldCaps.name, typeToReWrite, fieldCaps.isSearchable, fieldCaps.isAggregatable,
                                updatedIndices.toTypedArray(), fieldCaps.nonSearchableIndices(), fieldCaps.nonAggregatableIndices(), fieldCaps.meta())
                    } else {
                        rewrittenType[typeToReWrite] = FieldCapabilities(it, typeToReWrite, true, true, fieldsToUpdate.getValue(it).second.toTypedArray
                        (), arrayOf<String>(), arrayOf<String>(), mapOf<String, Set<String>>())
                    }
                    top[it] = rewrittenType
                } else {
                    top[it] = fields.getValue(it)
                }
            }
        }

        return top
    }

    private fun getSourceMappingsForRollupJob(rollupIndices: Set<String>) : MutableMap<RollupFieldMapping, MutableSet<String>> {
        val fieldMappingsMap = mutableMapOf<RollupFieldMapping, MutableSet<String>>()

        rollupIndices.forEach {
            val fieldMappings = populateRollupIndexFieldMappings(it)
            fieldMappings.forEach {
                it.value.forEach {fieldMapping ->
                    if (fieldMappingsMap[fieldMapping] == null) {
                        fieldMappingsMap[fieldMapping] = mutableSetOf()
                    }
                    fieldMappingsMap[fieldMapping]!!.add(it.key)
                }
            }
        }

        return fieldMappingsMap
    }

    private fun getFieldType(fieldName: String, mappings: Map<*, *>): String? {
        var currMap = mappings
        fieldName.split(".").forEach { field ->
            val nextMap = (currMap[IndexUtils.PROPERTIES] as Map<*, *>?)?.get(field) ?: return null
            currMap = nextMap as Map<*, *>
        }

        return currMap["type"]?.toString()
    }

    override fun order(): Int {
        return Integer.MIN_VALUE
    }
}