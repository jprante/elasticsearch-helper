/*
 * Copyright (C) 2015 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.helper.client;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Interface for providing convenient administrative methods for ingesting data into Elasticsearch.
 */
public interface ClientAPI extends ClientParameters {

    /**
     * Initialize new ingest client, wrap an existing Elasticsearch client, and set up metrics.
     *
     * @param client the Elasticsearch client
     * @param metric a metric or null
     * @return this ingest
     * @throws IOException if client could not get created
     */
    ClientAPI init(ElasticsearchClient client, IngestMetric metric) throws IOException;

    /**
     * Initialize, create new ingest client, and set up metrics.
     *
     * @param settings settings
     * @param metric a metric or null
     * @return this ingest
     * @throws IOException if client could not get created
     */
    ClientAPI init(Settings settings, IngestMetric metric) throws IOException;

    /**
     * Return Elasticsearch client
     *
     * @return Elasticsearch client
     */
    ElasticsearchClient client();

    /**
     * Index document
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    ClientAPI index(String index, String type, String id, String source);

    /**
     * Delete document
     *
     * @param index the index
     * @param type  the type
     * @param id    the id
     * @return this ingest
     */
    ClientAPI delete(String index, String type, String id);

    /**
     * Update document. Use with precaution! Does not work in all cases.
     *
     * @param index  the index
     * @param type   the type
     * @param id     the id
     * @param source the source
     * @return this
     */
    ClientAPI update(String index, String type, String id, String source);

    /**
     * Set the maximum number of actions per request
     *
     * @param maxActionsPerRequest maximum number of actions per request
     * @return this ingest
     */
    ClientAPI maxActionsPerRequest(int maxActionsPerRequest);

    /**
     * Set the maximum concurent requests
     *
     * @param maxConcurentRequests maximum number of concurrent ingest requests
     * @return this Ingest
     */
    ClientAPI maxConcurrentRequests(int maxConcurentRequests);

    /**
     * Set the maximum volume for request before flush
     *
     * @param maxVolume maximum volume
     * @return this ingest
     */
    ClientAPI maxVolumePerRequest(ByteSizeValue maxVolume);

    /**
     * Set the flush interval for automatic flushing outstanding ingest requests
     *
     * @param flushInterval the flush interval, default is 30 seconds
     * @return this ingest
     */
    ClientAPI flushIngestInterval(TimeValue flushInterval);

    /**
     * Set mapping
     *
     * @param type mapping type
     * @param in   mapping definition as input stream
     * @throws IOException if mapping could not be added
     */
    void mapping(String type, InputStream in) throws IOException;

    /**
     * Set mapping
     *
     * @param type    mapping type
     * @param mapping mapping definition as input stream
     * @throws IOException if mapping could not be added
     */
    void mapping(String type, String mapping) throws IOException;

    /**
     * Put mapping
     *
     * @param index index
     */
    void putMapping(String index);

    /**
     * Create a new index
     *
     * @param index index
     * @return this ingest
     */
    ClientAPI newIndex(String index);

    /**
     * Create a new index
     *
     * @param index    index
     * @param type     type
     * @param settings settings
     * @param mappings mappings
     * @return this ingest
     * @throws IOException if new index creation fails
     */
    ClientAPI newIndex(String index, String type, InputStream settings, InputStream mappings) throws IOException;

    /**
     * Create a new index
     *
     * @param index    index
     * @param settings settings
     * @param mappings mappings
     * @return this ingest
     */
    ClientAPI newIndex(String index, Settings settings, Map<String, String> mappings);

    /**
     * Create new mapping
     *
     * @param index   index
     * @param type    index type
     * @param mapping mapping
     * @return this ingest
     */
    ClientAPI newMapping(String index, String type, Map<String, Object> mapping);

    /**
     * Delete index
     *
     * @param index index
     * @return this ingest
     */
    ClientAPI deleteIndex(String index);

    /**
     * Start bulk mode
     *
     * @param index                index
     * @param startRefreshIntervalSeconds refresh interval before bulk
     * @param stopRefreshIntervalSeconds  refresh interval after bulk
     * @return this ingest
     * @throws IOException if bulk could not be started
     */
    ClientAPI startBulk(String index, long startRefreshIntervalSeconds, long stopRefreshIntervalSeconds) throws IOException;

    /**
     * Stops bulk mode
     *
     * @param index index
     * @return this Ingest
     * @throws IOException if bulk could not be stopped
     */
    ClientAPI stopBulk(String index) throws IOException;

    /**
     * Bulked index request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param indexRequest the index request to add
     * @return this ingest
     */
    ClientAPI bulkIndex(IndexRequest indexRequest);

    /**
     * Bulked delete request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     *
     * @param deleteRequest the delete request to add
     * @return this ingest
     */
    ClientAPI bulkDelete(DeleteRequest deleteRequest);

    /**
     * Bulked update request. Each request will be added to a queue for bulking requests.
     * Submitting request will be done when bulk limits are exceeded.
     * Note that updates only work correctly when all operations between nodes are synchronized!
     *
     * @param updateRequest the update request to add
     * @return this ingest
     */
    ClientAPI bulkUpdate(UpdateRequest updateRequest);

    /**
     * Flush ingest, move all pending documents to the cluster.
     *
     * @return this
     */
    ClientAPI flushIngest();

    /**
     * Wait for all outstanding responses
     *
     * @param maxWait maximum wait time
     * @return this ingest
     * @throws InterruptedException if wait is interrupted
     * @throws ExecutionException if execution failed
     */
    ClientAPI waitForResponses(TimeValue maxWait) throws InterruptedException, ExecutionException;

    /**
     * Refresh the index.
     *
     * @param index index
     */
    void refreshIndex(String index);

    /**
     * Flush the index.
     *
     * @param index index
     */
    void flushIndex(String index);

    /**
     * Update replica level.
     *
     * @param index index
     * @param level the replica level
     * @return number of shards after updating replica level
     * @throws IOException if replica could not be updated
     */
    int updateReplicaLevel(String index, int level) throws IOException;

    /**
     * Wait for cluster being healthy.
     *
     * @param healthColor    cluster health color to wait for
     * @param timeValue time value
     * @throws IOException if wait failed
     */
    void waitForCluster(String healthColor, TimeValue timeValue) throws IOException;

    /**
     * Get current health color
     * @return the cluster health color
     */
    String healthColor();

    /**
     * Wait for index recovery (after replica change)
     *
     * @param index index
     * @return number of shards found
     * @throws IOException if wait failed
     */
    int waitForRecovery(String index) throws IOException;

    /**
     * Resolve alias
     * @param alias the alias
     * @return one index name behind the alias or the alias if there is no index
     */
    String resolveAlias(String alias);

    /**
     * Resolve alias to all connected indices, sort index names with most recent timestamp on top, return this index name
     * @param alias the alias
     * @return the most recent index name pointing to the alias
     */
    String resolveMostRecentIndex(String alias);

    /**
     * Get all alias filters.
     * @param index index
     * @return map of alias filters
     */
    Map<String,String>  getAliasFilters(String index);

    /**
     * Switch aliases from one index to another.
     * @param index the index name
     * @param concreteIndex the index name with timestamp
     * @param extraAliases a list of names that should be set as index aliases
     */
    void switchAliases(String index, String concreteIndex, List<String> extraAliases);

    /**
     * Switch aliases from one index to another.
     * @param index the index name
     * @param concreteIndex the index name with timestamp
     * @param extraAliases a list of names that should be set as index aliases
     * @param adder an adder method to create alias term queries
     */
    void switchAliases(String index, String concreteIndex, List<String> extraAliases, IndexAliasAdder adder);

    /**
     * Retention policy for an index. All indices before timestampdiff should be deleted,
     * but mintokeep indices must be kept.
     * @param index index name
     * @param concreteIndex index name with timestamp
     * @param timestampdiff timestamp delta (for index timestamps)
     * @param mintokeep minimum number of indices to keep
     */
    void performRetentionPolicy(String index, String concreteIndex, int timestampdiff, int mintokeep);

    /**
     * Log the timestamp of the most recently indexed document in the index.
     * @param index the index name
     * @return millis UTC millis of the most recent document
     * @throws IOException if most rcent document can not be found
     */
    Long mostRecentDocument(String index) throws IOException;

    /**
     * Get metric
     *
     * @return metric
     */
    IngestMetric getMetric();

    /**
     * Returns true is a throwable exists
     *
     * @return true if a Throwable exists
     */
    boolean hasThrowable();

    /**
     * Return last throwable if exists.
     *
     * @return last throwable
     */
    Throwable getThrowable();

    /**
     * Shutdown the ingesting
     */
    void shutdown();
}
