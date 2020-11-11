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
            val indices = request.indices().map { it.toString() }.toTypedArray()
            val concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), *indices)
            val rollupIndices = mutableSetOf<String>()
            val filteredIndices = mutableSetOf<String>()
            for (index in concreteIndices) {
                val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)
                if (isRollupIndex) {
                    rollupIndices.add(index)
                } else {
                    filteredIndices.add(index)
                }
            }

            if (rollupIndices.size > 0) {
                logger.info("rewriting request to only contain filtered indices $filteredIndices")
                request.indices(*filteredIndices.toTypedArray())
            }

            val shouldShortCircuit = rollupIndices.size > 0 && filteredIndices.size == 0

            logger.info(request.indices().map {it.toString()}.toTypedArray())

            if (shouldShortCircuit) {
                val rewrittenResponse = rewriteResponse(mapOf(), arrayOf(), rollupIndices)
                listener.onResponse(rewrittenResponse as Response)
            } else {
                chain.proceed(task, action, request, object : ActionListener<Response> {
                    override fun onResponse(response: Response) {
                        if (rollupIndices.size > 0) {
                            logger.info("Need to rewrite field mapping response")
                            response as FieldCapabilitiesResponse
                            logger.info(request.indices().map {it.toString()}.toTypedArray())
                            val reWrittenResponse = rewriteResponse(response.get(), response.indices, rollupIndices)
                            listener.onResponse(reWrittenResponse as Response)
                        } else {
                            listener.onResponse(response)
                        }
                    }

                    override fun onFailure(e: Exception) {
                        listener.onFailure(e)
                    }
                })
            }
        } else {
            chain.proceed(task, action, request, listener)
        }

    }

    private fun rewriteResponse(
        fields: Map<String, Map<String, FieldCapabilities>>,
        indices: Array<String>,
        rollupIndices: Set<String>
    ): ActionResponse {
        val rollupFieldsIndexMap = getSourceMappingsForRollupJob(rollupIndices)
        /*val fieldsToRemove = mutableSetOf<String>()
        rollupFieldsIndexMap.keys.forEach {
            fieldsToRemove.addAll(it.internalFieldMappings())
        }
        fieldsToRemove.addAll(mutableSetOf("rollup", "rollup._id", "rollup._schema_version"))*/
        val fieldsToUpdate = populateFieldsToUpdate(rollupFieldsIndexMap)
        val rewritten = rewriteFieldsWithoutRemoving(fields, indices, fieldsToUpdate)

        val builder = XContentFactory.jsonBuilder().prettyPrint()
        builder.startObject()
        builder.field("indices", indices + rollupIndices)
        builder.field("fields", rewritten as Map<String, Any>?)
        builder.endObject()

        //logger.info(Strings.toString(builder))

        val parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler
                .THROW_UNSUPPORTED_OPERATION, Strings.toString(builder))
        //logger.info(reWrittenResponse.toString())
        return FieldCapabilitiesResponse.fromXContent(parser)
    }

    private fun populateFieldsToUpdate(fieldMappingIndexMap: Map<RollupFieldMapping, Set<String>>): Map<String, Map<String, Set<String>>> {
        val result = mutableMapOf<String, MutableMap<String, MutableSet<String>>>()
        fieldMappingIndexMap.keys.forEach {
            if (it.sourceType != null) {
                if (result[it.fieldName] == null) {
                    result[it.fieldName] = mutableMapOf()
                    result.getValue(it.fieldName)[it.sourceType!!] = mutableSetOf()
                }
                val typeIndicesMap = result.getValue(it.fieldName)
                if (!typeIndicesMap.keys.contains(it.sourceType!!)) {
                    typeIndicesMap[it.sourceType!!] = mutableSetOf()
                }
                typeIndicesMap.getValue(it.sourceType!!).addAll(fieldMappingIndexMap.getValue(it))
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

    private fun rewriteFieldsWithoutRemoving(
        fields: Map<String, Map<String, FieldCapabilities>>,
        filteredIndices: Array<String>,
        fieldsToUpdate: Map<String, Map<String, Set<String>>>
    ): Map<String, Map<String, FieldCapabilities>> {
        val filteredIndicesResponse = expandIndicesInResponse(filteredIndices, fields)
        val rollupIndicesResponse = populateResponseForRollupIndices(fieldsToUpdate)
        /* val rewritten = mutableMapOf<String, Map<String, FieldCapabilities>>()
           fields.keys.forEach {
            val rewrittenField = mutableMapOf<String, FieldCapabilities>()
            // writing all types for each field to output
            fields.getValue(it).keys.forEach { type ->
                rewrittenField[type] = fields.getValue(it).getValue(type)
            }

            if (fieldsToUpdate.keys.contains(it)) {
                logger.info("rewriting the field $it")

                fieldsToUpdate.getValue(it).keys.forEach { type ->
                    logger.info("rewriting the type $type")
                    // the field type exists so need to update
                    if (fields.getValue(it).keys.contains(type)) {
                        val fieldCaps = fields.getValue(it).getValue(type)
                        val indices = mutableSetOf<String>()
                        if (fieldCaps.indices() == null) {
                            indices.addAll(filteredIndices)
                        } else {
                            indices.addAll(fieldCaps.indices())
                        }
                        indices.addAll(fieldsToUpdate.getValue(it).getValue(type))
                        // TODO: should we check if the searchable and aggregatable match with rollup indices and consolidate?
                        // TODO: should we keep the other types in the response for the field or just the rollup field types?
                        rewrittenField[type] = FieldCapabilities(fieldCaps.name, type, fieldCaps.isSearchable, fieldCaps.isAggregatable,
                                indices.toTypedArray(), fieldCaps.nonSearchableIndices(), fieldCaps.nonAggregatableIndices(), fieldCaps.meta())
                    } else {
                        // the field type doesn't exist creating one
                        // TODO: should we determine aggregatable and searchable based whether the field is metric/dimension?
                        rewrittenField[type] = FieldCapabilities(it, type, true, true, fieldsToUpdate.getValue(it).getValue(type).toTypedArray(),
                                arrayOf<String>(), arrayOf<String>(), mapOf<String, Set<String>>())
                    }
                    logger.info("rewritten type $type is ${rewrittenField[type]}")
                }
                logger.info("rewritten field $it is now $rewrittenField")
            } else {
                logger.info("Not rewriting the field $it")
            }
            rewritten[it] = rewrittenField
        }*/

        return mergeResponses(filteredIndicesResponse, rollupIndicesResponse)
    }

    private fun expandIndicesInResponse(
        indices: Array<String>, response: Map<String, Map<String, FieldCapabilities>>): Map<String, Map<String, FieldCapabilities>> {
        val expandedResponse = mutableMapOf<String, MutableMap<String, FieldCapabilities>>()
        response.keys.forEach { field ->
            response.getValue(field).keys.forEach { type ->
                if (expandedResponse[field] == null) {
                    expandedResponse[field] = mutableMapOf()
                }
                val fieldCaps = response.getValue(field).getValue(type)
                val rewrittenIndices = if (fieldCaps.indices() != null && fieldCaps.indices().isNotEmpty()) fieldCaps.indices() else indices
                expandedResponse[field]!![type] = FieldCapabilities(fieldCaps.name, fieldCaps.type, fieldCaps.isSearchable, fieldCaps
                        .isAggregatable, rewrittenIndices, fieldCaps.nonSearchableIndices(), fieldCaps.nonAggregatableIndices(), fieldCaps.meta())
            }
        }

        return expandedResponse
    }

    private fun populateResponseForRollupIndices(rollupFields: Map<String, Map<String, Set<String>>>): Map<String, Map<String, FieldCapabilities>> {
        val response = mutableMapOf<String, MutableMap<String, FieldCapabilities>>()
        rollupFields.keys.forEach { field ->
            rollupFields.getValue(field).keys.forEach { type ->
                if (response[field] == null) {
                    response[field] = mutableMapOf()
                }
                response[field]!![type] = FieldCapabilities(field, type, true, true, rollupFields.getValue(field).getValue(type).toTypedArray(),
                        arrayOf<String>(), arrayOf<String>(), mapOf<String, Set<String>>())
            }
        }

        return response
    }

    private fun mergeResponses(
        r1: Map<String, Map<String, FieldCapabilities>>,
        r2: Map<String, Map<String, FieldCapabilities>>
    ): Map<String, Map<String, FieldCapabilities>> {
        val mergedResponses = mutableMapOf<String, Map<String, FieldCapabilities>>()
        val fields = r1.keys.union(r2.keys)
        fields.forEach { field ->
            val mergedFields = mergeFields(r1[field], r2[field])
            if (mergedFields != null) mergedResponses[field] = mergedFields
        }

        return mergedResponses
    }

     private fun mergeFields(f1: Map<String, FieldCapabilities>?, f2: Map<String, FieldCapabilities>?): Map<String, FieldCapabilities>? {
         if (f1 == null) return f2
         if (f2 == null) return f1
         val mergedFields = mutableMapOf<String, FieldCapabilities>()
         val types = f1.keys.union(f2.keys)
         types.forEach { type ->
             val mergedTypes = mergeTypes(f1[type], f2[type])
             if (mergedTypes != null) mergedFields[type] = mergedTypes
         }

         return mergedFields
    }

    private fun mergeTypes(t1: FieldCapabilities?, t2: FieldCapabilities?): FieldCapabilities? {
        if (t1 == null) return t2
        if (t2 == null) return t1
        if (t1.name != t2.name && t1.type != t2.type) {
            logger.warn("cannot merge $t1 and $t2")
            return null
        }
        val isSearchable = t1.isSearchable || t2.isSearchable
        val isAggregatable = t1.isAggregatable || t2.isAggregatable
        val name = t1.name
        val type = t1.type
        val indices = t1.indices() + t2. indices()
        val nonAggregatableIndices = arrayOf<String>() // t1.nonAggregatableIndices() + t2.nonAggregatableIndices()
        val nonSearchableIndices = arrayOf<String>() // t1.nonSearchableIndices() + t2.nonSearchableIndices()
        val meta = (t1.meta().keys + t2.meta().keys)
                .associateWith{ t1.meta().getOrDefault(it, mutableSetOf()).union(t2.meta().getOrDefault(it, mutableSetOf())) }

        return FieldCapabilities(name, type, isSearchable, isAggregatable, indices, nonSearchableIndices, nonAggregatableIndices, meta)
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
        return Integer.MAX_VALUE
    }
}