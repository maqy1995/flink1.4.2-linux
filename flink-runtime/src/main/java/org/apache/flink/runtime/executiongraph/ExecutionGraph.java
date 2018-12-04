/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.ExecutionGraphRestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The execution graph is the central data structure that coordinates the distributed
 * execution of a data flow. It keeps representations of each parallel task, each
 * intermediate stream, and the communication between them.
 *
 * <p>The execution graph consists of the following constructs:
 * <ul>
 *     <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one operation like
 *         "map" or "join") during execution. It holds the aggregated state of all parallel subtasks.
 *         The ExecutionJobVertex is identified inside the graph by the {@link JobVertexID}, which it takes
 *         from the JobGraph's corresponding JobVertex.</li>
 *     <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex, there are
 *         as many ExecutionVertices as the parallelism. The ExecutionVertex is identified by
 *         the ExecutionJobVertex and the number of the parallel subtask</li>
 *     <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple Executions
 *         for the ExecutionVertex, in case of a failure, or in the case where some data needs to be recomputed
 *         because it is no longer available when requested by later operations. An Execution is always
 *         identified by an {@link ExecutionAttemptID}. All messages between the JobManager and the TaskManager
 *         about deployment of tasks and updates in the task status always use the ExecutionAttemptID to
 *         address the message receiver.</li>
 * </ul>
 *
 * <h2>Global and local failover</h2>
 *
 * The Execution Graph has two failover modes: <i>global failover</i> and <i>local failover</i>.
 *
 * <p>A <b>global failover</b> aborts the task executions for all vertices and restarts whole
 * data flow graph from the last completed checkpoint. Global failover is considered the
 * "fallback strategy" that is used when a local failover is unsuccessful, or when a issue is
 * found in the state of the ExecutionGraph that could mark it as inconsistent (caused by a bug).
 *
 * <p>A <b>local failover</b> is triggered when an individual vertex execution (a task) fails.
 * The local failover is coordinated by the {@link FailoverStrategy}. A local failover typically
 * attempts to restart as little as possible, but as much as necessary.
 *
 * <p>Between local- and global failover, the global failover always takes precedence, because it
 * is the core mechanism that the ExecutionGraph relies on to bring back consistency. The
 * guard that, the ExecutionGraph maintains a <i>global modification version</i>, which is incremented
 * with every global failover (and other global actions, like job cancellation, or terminal
 * failure). Local failover is always scoped by the modification version that the execution graph
 * had when the failover was triggered. If a new global modification version is reached during
 * local failover (meaning there is a concurrent global failover), the failover strategy has to
 * yield before the global failover.
 */
public class ExecutionGraph implements AccessExecutionGraph, Archiveable<ArchivedExecutionGraph> {

	/** In place updater for the execution graph's current state. Avoids having to use an
	 * AtomicReference and thus makes the frequent read access a bit faster */
	private static final AtomicReferenceFieldUpdater<ExecutionGraph, JobStatus> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(ExecutionGraph.class, JobStatus.class, "state");

	/** In place updater for the execution graph's current global recovery version.
	 * Avoids having to use an AtomicLong and thus makes the frequent read access a bit faster */
	private static final AtomicLongFieldUpdater<ExecutionGraph> GLOBAL_VERSION_UPDATER =
			AtomicLongFieldUpdater.newUpdater(ExecutionGraph.class, "globalModVersion");

	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

	// --------------------------------------------------------------------------------------------

	/** The lock used to secure all access to mutable fields, especially the tracking of progress
	 * within the job. */
	private final Object progressLock = new Object();

	/** Job specific information like the job id, job name, job configuration, etc. */
	private final JobInformation jobInformation;

	/** Serialized job information or a blob key pointing to the offloaded job information */
	private final Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey;

	/** The executor which is used to execute futures. */
	private final ScheduledExecutorService futureExecutor;

	/** The executor which is used to execute blocking io operations */
	private final Executor ioExecutor;

	/** {@code true} if all source tasks are stoppable. */
	private boolean isStoppable = true;

	/** All job vertices that are part of this graph */
	private final ConcurrentHashMap<JobVertexID, ExecutionJobVertex> tasks;

	/** All vertices, in the order in which they were created **/
	private final List<ExecutionJobVertex> verticesInCreationOrder;

	/** All intermediate results that are part of this graph */
	private final ConcurrentHashMap<IntermediateDataSetID, IntermediateResult> intermediateResults;

	/** The currently executed tasks, for callbacks */
	private final ConcurrentHashMap<ExecutionAttemptID, Execution> currentExecutions;

	/** Listeners that receive messages when the entire job switches it status
	 * (such as from RUNNING to FINISHED) */
	private final List<JobStatusListener> jobStatusListeners;

	/** Listeners that receive messages whenever a single task execution changes its status */
	private final List<ExecutionStatusListener> executionListeners;

	/** The implementation that decides how to recover the failures of tasks */
	private final FailoverStrategy failoverStrategy;

	/** Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when
	 * the execution graph transitioned into a certain state. The index into this array is the
	 * ordinal of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is
	 * at {@code stateTimestamps[RUNNING.ordinal()]}. */
	private final long[] stateTimestamps;

	/** The timeout for all messages that require a response/acknowledgement */
	private final Time rpcCallTimeout;

	/** The timeout for bulk slot allocation (eager scheduling mode). After this timeout,
	 * slots are released and a recovery is triggered */
	private final Time scheduleAllocationTimeout;

	/** Strategy to use for restarts */
	private final RestartStrategy restartStrategy;

	/** The slot provider to use for allocating slots for tasks as they are needed */
	private final SlotProvider slotProvider;

	/** The classloader for the user code. Needed for calls into user code classes */
	private final ClassLoader userClassLoader;

	/** Registered KvState instances reported by the TaskManagers. */
	private final KvStateLocationRegistry kvStateLocationRegistry;

	/** Blob writer used to offload RPC messages */
	private final BlobWriter blobWriter;

	/** The total number of vertices currently in the execution graph */
	private int numVerticesTotal;

	// ------ Configuration of the Execution -------

	/** Flag to indicate whether the scheduler may queue tasks for execution, or needs to be able
	 * to deploy them immediately. */
	private boolean allowQueuedScheduling = false;

	/** The mode of scheduling. Decides how to select the initial set of tasks to be deployed.
	 * May indicate to deploy all sources, or to deploy everything, or to deploy via backtracking
	 * from results than need to be materialized. */
	private ScheduleMode scheduleMode = ScheduleMode.LAZY_FROM_SOURCES;

	// ------ Execution status and progress. These values are volatile, and accessed under the lock -------

	private final AtomicInteger verticesFinished;

	/** Current status of the job execution */
	private volatile JobStatus state = JobStatus.CREATED;

	/** A future that completes once the job has reached a terminal state */
	private volatile CompletableFuture<JobStatus> terminationFuture;

	/** On each global recovery, this version is incremented. The version breaks conflicts
	 * between concurrent restart attempts by local failover strategies */
	private volatile long globalModVersion;

	/** The exception that caused the job to fail. This is set to the first root exception
	 * that was not recoverable and triggered job failure */
	private volatile Throwable failureCause;

	/** The extended failure cause information for the job. This exists in addition to 'failureCause',
	 * to let 'failureCause' be a strong reference to the exception, while this info holds no
	 * strong reference to any user-defined classes.*/
	private volatile ErrorInfo failureInfo;

	// ------ Fields that are relevant to the execution and need to be cleared before archiving  -------

	/** The coordinator for checkpoints, if snapshot checkpoints are enabled */
	private CheckpointCoordinator checkpointCoordinator;

	/** Checkpoint stats tracker separate from the coordinator in order to be
	 * available after archiving. */
	private CheckpointStatsTracker checkpointStatsTracker;

	// ------ Fields that are only relevant for archived execution graphs ------------
	private String jsonPlan;

	// --------------------------------------------------------------------------------------------
	//   Constructors
	// --------------------------------------------------------------------------------------------

	/**
	 * This constructor is for tests only, because it sets default values for many fields.
	 */
	@VisibleForTesting
	ExecutionGraph(
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			JobID jobId,
			String jobName,
			Configuration jobConfig,
			SerializedValue<ExecutionConfig> serializedConfig,
			Time timeout,
			RestartStrategy restartStrategy,
			SlotProvider slotProvider) throws IOException {

		this(
			new JobInformation(
				jobId,
				jobName,
				serializedConfig,
				jobConfig,
				Collections.emptyList(),
				Collections.emptyList()),
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			slotProvider);
	}

	/**
	 * This constructor is for tests only, because it does not include class loading information.
	 */
	@VisibleForTesting
	ExecutionGraph(
			JobInformation jobInformation,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			Time timeout,
			RestartStrategy restartStrategy,
			SlotProvider slotProvider) throws IOException {
		this(
			jobInformation,
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			new RestartAllStrategy.Factory(),
			slotProvider);
	}

	@VisibleForTesting
	ExecutionGraph(
			JobInformation jobInformation,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			Time timeout,
			RestartStrategy restartStrategy,
			FailoverStrategy.Factory failoverStrategy,
			SlotProvider slotProvider) throws IOException {
		this(
			jobInformation,
			futureExecutor,
			ioExecutor,
			timeout,
			restartStrategy,
			failoverStrategy,
			slotProvider,
			ExecutionGraph.class.getClassLoader(),
			VoidBlobWriter.getInstance());
	}

	public ExecutionGraph(
			JobInformation jobInformation,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			Time timeout,
			RestartStrategy restartStrategy,
			FailoverStrategy.Factory failoverStrategyFactory,
			SlotProvider slotProvider,
			ClassLoader userClassLoader,
			BlobWriter blobWriter) throws IOException {

		checkNotNull(futureExecutor);

		this.jobInformation = Preconditions.checkNotNull(jobInformation);

		this.blobWriter = Preconditions.checkNotNull(blobWriter);

		this.jobInformationOrBlobKey = BlobWriter.serializeAndTryOffload(jobInformation, jobInformation.getJobId(), blobWriter);

		this.futureExecutor = Preconditions.checkNotNull(futureExecutor);
		this.ioExecutor = Preconditions.checkNotNull(ioExecutor);

		this.slotProvider = Preconditions.checkNotNull(slotProvider, "scheduler");
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader, "userClassLoader");

		this.tasks = new ConcurrentHashMap<>(16);
		this.intermediateResults = new ConcurrentHashMap<>(16);
		this.verticesInCreationOrder = new ArrayList<>(16);
		this.currentExecutions = new ConcurrentHashMap<>(16);

		this.jobStatusListeners  = new CopyOnWriteArrayList<>();
		this.executionListeners = new CopyOnWriteArrayList<>();

		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.rpcCallTimeout = checkNotNull(timeout);
		this.scheduleAllocationTimeout = checkNotNull(timeout);

		this.restartStrategy = restartStrategy;
		this.kvStateLocationRegistry = new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

		this.verticesFinished = new AtomicInteger();

		this.globalModVersion = 1L;

		// the failover strategy must be instantiated last, so that the execution graph
		// is ready by the time the failover strategy sees it
		this.failoverStrategy = checkNotNull(failoverStrategyFactory.create(this), "null failover strategy");
		LOG.info("Job recovers via failover strategy: {}", failoverStrategy.getStrategyName());
	}

	// --------------------------------------------------------------------------------------------
	//  Configuration of Data-flow wide execution settings
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the number of job vertices currently held by this execution graph.
	 * @return The current number of job vertices.
	 */
	public int getNumberOfExecutionJobVertices() {
		return this.verticesInCreationOrder.size();
	}

	public boolean isQueuedSchedulingAllowed() {
		return this.allowQueuedScheduling;
	}

	public void setQueuedSchedulingAllowed(boolean allowed) {
		this.allowQueuedScheduling = allowed;
	}

	public void setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	@Override
	public boolean isArchived() {
		return false;
	}

	public void enableCheckpointing(
			long interval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			ExternalizedCheckpointSettings externalizeSettings,
			List<ExecutionJobVertex> verticesToTrigger,
			List<ExecutionJobVertex> verticesToWaitFor,
			List<ExecutionJobVertex> verticesToCommitTo,
			List<MasterTriggerRestoreHook<?>> masterHooks,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore checkpointStore,
			String checkpointDir,
			StateBackend metadataStore,
			CheckpointStatsTracker statsTracker) {

		// simple sanity checks
		checkArgument(interval >= 10, "checkpoint interval must not be below 10ms");
		checkArgument(checkpointTimeout >= 10, "checkpoint timeout must not be below 10ms");

		checkState(state == JobStatus.CREATED, "Job must be in CREATED state");
		checkState(checkpointCoordinator == null, "checkpointing already enabled");

		ExecutionVertex[] tasksToTrigger = collectExecutionVertices(verticesToTrigger);
		ExecutionVertex[] tasksToWaitFor = collectExecutionVertices(verticesToWaitFor);
		ExecutionVertex[] tasksToCommitTo = collectExecutionVertices(verticesToCommitTo);

		checkpointStatsTracker = checkNotNull(statsTracker, "CheckpointStatsTracker");

		// create the coordinator that triggers and commits checkpoints and holds the state
		checkpointCoordinator = new CheckpointCoordinator(
			jobInformation.getJobId(),
			interval,
			checkpointTimeout,
			minPauseBetweenCheckpoints,
			maxConcurrentCheckpoints,
			externalizeSettings,
			tasksToTrigger,
			tasksToWaitFor,
			tasksToCommitTo,
			checkpointIDCounter,
			checkpointStore,
			checkpointDir,
			ioExecutor,
			SharedStateRegistry.DEFAULT_FACTORY);

		// register the master hooks on the checkpoint coordinator
		for (MasterTriggerRestoreHook<?> hook : masterHooks) {
			if (!checkpointCoordinator.addMasterHook(hook)) {
				LOG.warn("Trying to register multiple checkpoint hooks with the name: {}", hook.getIdentifier());
			}
		}

		checkpointCoordinator.setCheckpointStatsTracker(checkpointStatsTracker);

		// interval of max long value indicates disable periodic checkpoint,
		// the CheckpointActivatorDeactivator should be created only if the interval is not max value
		if (interval != Long.MAX_VALUE) {
			// the periodic checkpoint scheduler is activated and deactivated as a result of
			// job status changes (running -> on, all other states -> off)
			registerJobStatusListener(checkpointCoordinator.createActivatorDeactivator());
		}
	}

	public CheckpointCoordinator getCheckpointCoordinator() {
		return checkpointCoordinator;
	}

	public KvStateLocationRegistry getKvStateLocationRegistry() {
		return kvStateLocationRegistry;
	}

	public RestartStrategy getRestartStrategy() {
		return restartStrategy;
	}

	@Override
	public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
		if (checkpointStatsTracker != null) {
			return checkpointStatsTracker.getJobCheckpointingConfiguration();
		} else {
			return null;
		}
	}

	@Override
	public CheckpointStatsSnapshot getCheckpointStatsSnapshot() {
		if (checkpointStatsTracker != null) {
			return checkpointStatsTracker.createSnapshot();
		} else {
			return null;
		}
	}

	private ExecutionVertex[] collectExecutionVertices(List<ExecutionJobVertex> jobVertices) {
		if (jobVertices.size() == 1) {
			ExecutionJobVertex jv = jobVertices.get(0);
			if (jv.getGraph() != this) {
				throw new IllegalArgumentException("Can only use ExecutionJobVertices of this ExecutionGraph");
			}
			return jv.getTaskVertices();
		}
		else {
			ArrayList<ExecutionVertex> all = new ArrayList<>();
			for (ExecutionJobVertex jv : jobVertices) {
				if (jv.getGraph() != this) {
					throw new IllegalArgumentException("Can only use ExecutionJobVertices of this ExecutionGraph");
				}
				all.addAll(Arrays.asList(jv.getTaskVertices()));
			}
			return all.toArray(new ExecutionVertex[all.size()]);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Properties and Status of the Execution Graph
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a list of BLOB keys referring to the JAR files required to run this job
	 * @return list of BLOB keys referring to the JAR files required to run this job
	 */
	public Collection<PermanentBlobKey> getRequiredJarFiles() {
		return jobInformation.getRequiredJarFileBlobKeys();
	}

	/**
	 * Returns a list of classpaths referring to the directories/JAR files required to run this job
	 * @return list of classpaths referring to the directories/JAR files required to run this job
	 */
	public Collection<URL> getRequiredClasspaths() {
		return jobInformation.getRequiredClasspathURLs();
	}

	// --------------------------------------------------------------------------------------------

	public void setJsonPlan(String jsonPlan) {
		this.jsonPlan = jsonPlan;
	}

	@Override
	public String getJsonPlan() {
		return jsonPlan;
	}

	public SlotProvider getSlotProvider() {
		return slotProvider;
	}

	public Either<SerializedValue<JobInformation>, PermanentBlobKey> getJobInformationOrBlobKey() {
		return jobInformationOrBlobKey;
	}

	@Override
	public JobID getJobID() {
		return jobInformation.getJobId();
	}

	@Override
	public String getJobName() {
		return jobInformation.getJobName();
	}

	@Override
	public boolean isStoppable() {
		return this.isStoppable;
	}

	public Configuration getJobConfiguration() {
		return jobInformation.getJobConfiguration();
	}

	public ClassLoader getUserClassLoader() {
		return this.userClassLoader;
	}

	@Override
	public JobStatus getState() {
		return state;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	public ErrorInfo getFailureInfo() {
		return failureInfo;
	}

	/**
	 * Gets the number of full restarts that the execution graph went through.
	 * If a full restart recovery is currently pending, this recovery is included in the
	 * count.
	 *
	 * @return The number of full restarts so far
	 */
	public long getNumberOfFullRestarts() {
		// subtract one, because the version starts at one
		return globalModVersion - 1;
	}

	@Override
	public ExecutionJobVertex getJobVertex(JobVertexID id) {
		return this.tasks.get(id);
	}

	@Override
	public Map<JobVertexID, ExecutionJobVertex> getAllVertices() {
		return Collections.unmodifiableMap(this.tasks);
	}

	@Override
	public Iterable<ExecutionJobVertex> getVerticesTopologically() {
		// we return a specific iterator that does not fail with concurrent modifications
		// the list is append only, so it is safe for that
		final int numElements = this.verticesInCreationOrder.size();

		return new Iterable<ExecutionJobVertex>() {
			@Override
			public Iterator<ExecutionJobVertex> iterator() {
				return new Iterator<ExecutionJobVertex>() {
					private int pos = 0;

					@Override
					public boolean hasNext() {
						return pos < numElements;
					}

					@Override
					public ExecutionJobVertex next() {
						if (hasNext()) {
							return verticesInCreationOrder.get(pos++);
						} else {
							throw new NoSuchElementException();
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	public int getTotalNumberOfVertices() {
		return numVerticesTotal;
	}

	public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
		return Collections.unmodifiableMap(this.intermediateResults);
	}

	@Override
	public Iterable<ExecutionVertex> getAllExecutionVertices() {
		return new Iterable<ExecutionVertex>() {
			@Override
			public Iterator<ExecutionVertex> iterator() {
				return new AllVerticesIterator(getVerticesTopologically().iterator());
			}
		};
	}

	@Override
	public long getStatusTimestamp(JobStatus status) {
		return this.stateTimestamps[status.ordinal()];
	}

	public final BlobWriter getBlobWriter() {
		return blobWriter;
	}

	/**
	 * Returns the ExecutionContext associated with this ExecutionGraph.
	 *
	 * @return ExecutionContext associated with this ExecutionGraph
	 */
	public Executor getFutureExecutor() {
		return futureExecutor;
	}

	/**
	 * Merges all accumulator results from the tasks previously executed in the Executions.
	 * @return The accumulator map
	 */
	public Map<String, Accumulator<?,?>> aggregateUserAccumulators() {

		Map<String, Accumulator<?, ?>> userAccumulators = new HashMap<>();

		for (ExecutionVertex vertex : getAllExecutionVertices()) {
			Map<String, Accumulator<?, ?>> next = vertex.getCurrentExecutionAttempt().getUserAccumulators();
			if (next != null) {
				AccumulatorHelper.mergeInto(userAccumulators, next);
			}
		}

		return userAccumulators;
	}

	/**
	 * Gets the accumulator results.
	 */
	public Map<String, Object> getAccumulators() throws IOException {

		Map<String, Accumulator<?, ?>> accumulatorMap = aggregateUserAccumulators();

		Map<String, Object> result = new HashMap<>();
		for (Map.Entry<String, Accumulator<?, ?>> entry : accumulatorMap.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getLocalValue());
		}

		return result;
	}

	/**
	 * Gets a serialized accumulator map.
	 * @return The accumulator map with serialized accumulator values.
	 * @throws IOException
	 */
	@Override
	public Map<String, SerializedValue<Object>> getAccumulatorsSerialized() throws IOException {

		Map<String, Accumulator<?, ?>> accumulatorMap = aggregateUserAccumulators();

		Map<String, SerializedValue<Object>> result = new HashMap<>(accumulatorMap.size());
		for (Map.Entry<String, Accumulator<?, ?>> entry : accumulatorMap.entrySet()) {
			result.put(entry.getKey(), new SerializedValue<>(entry.getValue().getLocalValue()));
		}

		return result;
	}

	/**
	 * Returns the a stringified version of the user-defined accumulators.
	 * @return an Array containing the StringifiedAccumulatorResult objects
	 */
	@Override
	public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
		Map<String, Accumulator<?, ?>> accumulatorMap = aggregateUserAccumulators();
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {

		LOG.debug("Attaching {} topologically sorted vertices to existing job graph with {} " +
				"vertices and {} intermediate results.",
				topologiallySorted.size(), tasks.size(), intermediateResults.size());

		final ArrayList<ExecutionJobVertex> newExecJobVertices = new ArrayList<>(topologiallySorted.size());
		final long createTimestamp = System.currentTimeMillis();

		//依次顺序遍历排好序的JobVertex集合
		for (JobVertex jobVertex : topologiallySorted) {

			//对于ExecutionGraph来说,只要有一个不能停止的输入源JobVertex,那ExecutionGraph就是不可停止的
			if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
				this.isStoppable = false; //不知道这个值是用来干嘛的
			}

			// create the execution job vertex and attach it to the graph
			// 在ExecutionJobVertex的构造器中，将JobVertex的相关信息赋给了ExecutionJobVertex
			// 创建jobVertex对应的ExecutionJobVertex,其中的第三个构造参数1,就是默认的并行度
			// 在ExecutionJobVertex的构造函数中,会根据并行度,构造相应的ExecutionVertex数组,该数组的索引就是子任务的索引号;
			ExecutionJobVertex ejv = new ExecutionJobVertex(
				this,
				jobVertex,
				1,
				rpcCallTimeout,
				globalModVersion,
				createTimestamp);

			// 将新建的ExecutionJobVertex实例, 与其前置处理器建立连接
			ejv.connectToPredecessors(this.intermediateResults);   //这个intermediateResults第一次还没有初始化，834行会进行添加
			//通过上一行将每个ExecutionJobVertex与之前的进行了连接，
			//将构建好的ejv,记录下来,如果发现对一个的jobVertexID已经存在一个ExecutionJobVertex,则需要抛异常
			ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv); //将该ExecutionJobVertex放入
			if (previousTask != null) {                                              //返回值不为空说明有两个相同ID的vertex，报错。
				throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
						jobVertex.getID(), ejv, previousTask));
			}

			//将这个ExecutionGraph中所有临时结果IntermediateResult, 都保存到intermediateResults这个map,同样，如果之前就已经有了，说明有问题
			for (IntermediateResult res : ejv.getProducedDataSets()) {
				IntermediateResult previousDataSet = this.intermediateResults.putIfAbsent(res.getId(), res);
				if (previousDataSet != null) {
					throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
							res.getId(), res, previousDataSet));
				}
			}

			//将ejv按创建顺序记录下来
			this.verticesInCreationOrder.add(ejv);
			//统计所有ejv的并行度
			this.numVerticesTotal += ejv.getParallelism();
			newExecJobVertices.add(ejv);
		}

		//上述的逻辑是比较清晰的,就是依次遍历排好序的JobVertex集合,并构建相应的ExecutionJobVertex实例,并设置ExecutionGraph中的部分属性。
		terminationFuture = new CompletableFuture<>();
		failoverStrategy.notifyNewVertices(newExecJobVertices);
	}

	public void scheduleForExecution() throws JobException {

		// 将状态从'CREATED’转换为’RUNNING
		// 状态转换成功,会给状态监听者发送状态变化的消息,然后就根据调度的不同模式,进行不同的调度
		if (transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {

			//根据调度模式,执行不同的调度策略
			switch (scheduleMode) {

				//slotProvider是一个接口，JobManager中的Scheduler实现了该接口
				case LAZY_FROM_SOURCES:
					scheduleLazy(slotProvider); //该模式下,从source节点开始部署执行,成功后,再部署其下游节点,以此类推;批处理是该模式
					break;

				case EAGER:
					scheduleEager(slotProvider, scheduleAllocationTimeout);
					break;

				default:
					throw new JobException("Schedule mode is invalid.");
			}
		}
		else {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}

	private void scheduleLazy(SlotProvider slotProvider) {
		// simply take the vertices without inputs.
		for (ExecutionJobVertex ejv : verticesInCreationOrder) {
			if (ejv.getJobVertex().isInputVertex()) { //如果是源节点，则进行调度
				ejv.scheduleAll(
					slotProvider,
					allowQueuedScheduling,
					LocationPreferenceConstraint.ALL); // since it is an input vertex, the input based location preferences should be empty
			}
		}
	}

	/**
	 *
	 *
	 * @param slotProvider  The resource provider from which the slots are allocated
	 * @param timeout       The maximum time that the deployment may take, before a
	 *                      TimeoutException is thrown.
	 *
	 * 整个处理逻辑分为两大步骤:
	 *
	 * a、先进行槽位的分配,获取分配的future;
	 * b、成功获取槽位之后,进行部署,这步也是异步的;
	 *
	 * 另外,在槽位分配上,加上了超时机制,如果达到设定时间,槽位还没有分配好,则进行fail操作
	 */
	private void scheduleEager(SlotProvider slotProvider, final Time timeout) {
		//走到这里了,需要再次确认下当前的状态是否是'RUNNING'
		checkState(state == JobStatus.RUNNING, "job is not running currently");

		// Important: reserve all the space we need up front.保留我们需要的所有空间
		// that way we do not have any operation that can fail between allocating the slots
		// and adding them to the list. If we had a failure in between there, that would
		// cause the slots to get lost
		//标识在无法立即获取部署资源时,是否可以将部署任务入队列
		final boolean queued = allowQueuedScheduling;

		// collecting all the slots may resize and fail in that operation without slots getting lost
		// 用来维护所有槽位申请的future
		final ArrayList<CompletableFuture<Execution>> allAllocationFutures = new ArrayList<>(getNumberOfExecutionJobVertices());

		// allocate the slots (obtain all their futures
		// 获取每个ExecutionJobGraph申请槽位的future
		for (ExecutionJobVertex ejv : getVerticesTopologically()) {
			// these calls are not blocking, they only return futures
			// 槽位的申请分配逻辑
			Collection<CompletableFuture<Execution>> allocationFutures = ejv.allocateResourcesForAll(
				slotProvider,
				queued,
				LocationPreferenceConstraint.ALL);//ALL :wait for all inputs to have a location assigned

			allAllocationFutures.addAll(allocationFutures);
		}

		// this future is complete once all slot futures are complete.
		// the future fails once one slot future fails.
		// 将上面的所有future连接成一个future,只有所有的future都成功,才算成功,否则就是失败的
		final ConjunctFuture<Collection<Execution>> allAllocationsComplete = FutureUtils.combineAll(allAllocationFutures);

		// make sure that we fail if the allocation timeout was exceeded
		// 构建一个定时任务,用来检查槽位分配是否超时
		final ScheduledFuture<?> timeoutCancelHandle = futureExecutor.schedule(new Runnable() {
			@Override
			public void run() {
				// When the timeout triggers, we try to complete the conjunct future with an exception.
				// Note that this is a no-op if the future is already completed
				int numTotal = allAllocationsComplete.getNumFuturesTotal();
				int numComplete = allAllocationsComplete.getNumFuturesCompleted();
				String message = "Could not allocate all requires slots within timeout of " +
						timeout + ". Slots required: " + numTotal + ", slots allocated: " + numComplete;

				//如果超时,则以异常的方式结束分配
				allAllocationsComplete.completeExceptionally(new NoResourceAvailableException(message));
			}
		}, timeout.getSize(), timeout.getUnit());


		//根据槽位分配,进行异步调用执行
		allAllocationsComplete.handleAsync(
			(Collection<Execution> executions, Throwable throwable) -> {
				try {
					// we do not need the cancellation timeout any more
					// 取消上面的超时检查任务
					timeoutCancelHandle.cancel(false);

					if (throwable == null) {
						// successfully obtained all slots, now deploy
						// 成功获取所需槽位, 现在开始部署
						for (Execution execution : executions) {
							execution.deploy();
						}
					}
					else {
						// let the exception handler deal with this
						//抛出异常, 让异常句柄处理这个
						throw throwable;
					}
				}
				catch (Throwable t) {
					// we catch everything here to make sure cleanup happens and the
					// ExecutionGraph notices the error
					failGlobal(ExceptionUtils.stripCompletionException(t));
				}

				// Wouldn't it be nice if we could return an actual Void object?
				// return (Void) Unsafe.getUnsafe().allocateInstance(Void.class);
				return null;
			},
			futureExecutor);
	}

	public void cancel() {
		while (true) {
			JobStatus current = state;

			if (current == JobStatus.RUNNING || current == JobStatus.CREATED) {
				if (transitionState(current, JobStatus.CANCELLING)) {

					// make sure no concurrent local actions interfere with the cancellation
					final long globalVersionForRestart = incrementGlobalModVersion();

					final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(verticesInCreationOrder.size());

					// cancel all tasks (that still need cancelling)
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						futures.add(ejv.cancelWithFuture());
					}

					// we build a future that is complete once all vertices have reached a terminal state
					final ConjunctFuture<Void> allTerminal = FutureUtils.waitForAll(futures);
					allTerminal.thenAccept(
						(Void value) -> {
							// cancellations may currently be overridden by failures which trigger
							// restarts, so we need to pass a proper restart global version here
							allVerticesInTerminalState(globalVersionForRestart);
						}
					);

					return;
				}
			}
			// Executions are being canceled. Go into cancelling and wait for
			// all vertices to be in their final state.
			else if (current == JobStatus.FAILING) {
				if (transitionState(current, JobStatus.CANCELLING)) {
					return;
				}
			}
			// All vertices have been cancelled and it's safe to directly go
			// into the canceled state.
			else if (current == JobStatus.RESTARTING) {
				synchronized (progressLock) {
					if (transitionState(current, JobStatus.CANCELED)) {
						onTerminalState(JobStatus.CANCELED);

						LOG.info("Canceled during restart.");
						return;
					}
				}
			}
			else {
				// no need to treat other states
				return;
			}
		}
	}

	public void stop() throws StoppingException {
		if (isStoppable) {
			for (ExecutionVertex ev : this.getAllExecutionVertices()) {
				if (ev.getNumberOfInputs() == 0) { // send signal to sources only
					ev.stop();
				}
			}
		} else {
			throw new StoppingException("This job is not stoppable.");
		}
	}

	/**
	 * Suspends the current ExecutionGraph.
	 *
	 * The JobStatus will be directly set to SUSPENDED iff the current state is not a terminal
	 * state. All ExecutionJobVertices will be canceled and the onTerminalState() is executed.
	 *
	 * The SUSPENDED state is a local terminal state which stops the execution of the job but does
	 * not remove the job from the HA job store so that it can be recovered by another JobManager.
	 *
	 * @param suspensionCause Cause of the suspension
	 */
	public void suspend(Throwable suspensionCause) {
		while (true) {
			JobStatus currentState = state;

			if (currentState.isTerminalState()) {
				// stay in a terminal state
				return;
			} else if (transitionState(currentState, JobStatus.SUSPENDED, suspensionCause)) {
				initFailureCause(suspensionCause);

				// make sure no concurrent local actions interfere with the cancellation
				incrementGlobalModVersion();

				for (ExecutionJobVertex ejv: verticesInCreationOrder) {
					ejv.cancel();
				}

				synchronized (progressLock) {
					onTerminalState(JobStatus.SUSPENDED);

					LOG.info("Job {} has been suspended.", getJobID());
				}

				return;
			}
		}
	}

	/**
	 * Fails the execution graph globally. This failure will not be recovered by a specific
	 * failover strategy, but results in a full restart of all tasks.
	 *
	 * <p>This global failure is meant to be triggered in cases where the consistency of the
	 * execution graph' state cannot be guaranteed any more (for example when catching unexpected
	 * exceptions that indicate a bug or an unexpected call race), and where a full restart is the
	 * safe way to get consistency back.
	 *
	 * @param t The exception that caused the failure.
	 */
	public void failGlobal(Throwable t) {
		while (true) {
			JobStatus current = state;
			// stay in these states
			if (current == JobStatus.FAILING ||
				current == JobStatus.SUSPENDED ||
				current.isGloballyTerminalState()) {
				return;
			}
			else if (current == JobStatus.RESTARTING) {
				// we handle 'failGlobal()' while in 'RESTARTING' as a safety net in case something
				// has gone wrong in 'RESTARTING' and we need to re-attempt the restarts
				initFailureCause(t);

				final long globalVersionForRestart = incrementGlobalModVersion();
				if (tryRestartOrFail(globalVersionForRestart)) {
					return;
				}
			}
			else if (transitionState(current, JobStatus.FAILING, t)) {
				initFailureCause(t);

				// make sure no concurrent local or global actions interfere with the failover
				final long globalVersionForRestart = incrementGlobalModVersion();

				// we build a future that is complete once all vertices have reached a terminal state
				final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(verticesInCreationOrder.size());

				// cancel all tasks (that still need cancelling)
				for (ExecutionJobVertex ejv : verticesInCreationOrder) {
					futures.add(ejv.cancelWithFuture());
				}

				final ConjunctFuture<Void> allTerminal = FutureUtils.waitForAll(futures);
				allTerminal.thenAccept((Void value) -> allVerticesInTerminalState(globalVersionForRestart));

				return;
			}

			// else: concurrent change to execution state, retry
		}
	}

	public void restart(long expectedGlobalVersion) {
		try {
			synchronized (progressLock) {
				// check the global version to see whether this recovery attempt is still valid
				if (globalModVersion != expectedGlobalVersion) {
					LOG.info("Concurrent full restart subsumed this restart.");
					return;
				}

				final JobStatus current = state;

				if (current == JobStatus.CANCELED) {
					LOG.info("Canceled job during restart. Aborting restart.");
					return;
				} else if (current == JobStatus.FAILED) {
					LOG.info("Failed job during restart. Aborting restart.");
					return;
				} else if (current == JobStatus.SUSPENDED) {
					LOG.info("Suspended job during restart. Aborting restart.");
					return;
				} else if (current != JobStatus.RESTARTING) {
					throw new IllegalStateException("Can only restart job from state restarting.");
				}

				this.currentExecutions.clear();

				final Collection<CoLocationGroup> colGroups = new HashSet<>();
				final long resetTimestamp = System.currentTimeMillis();

				for (ExecutionJobVertex jv : this.verticesInCreationOrder) {

					CoLocationGroup cgroup = jv.getCoLocationGroup();
					if (cgroup != null && !colGroups.contains(cgroup)){
						cgroup.resetConstraints();
						colGroups.add(cgroup);
					}

					jv.resetForNewExecution(resetTimestamp, globalModVersion);
				}

				for (int i = 0; i < stateTimestamps.length; i++) {
					if (i != JobStatus.RESTARTING.ordinal()) {
						// Only clear the non restarting state in order to preserve when the job was
						// restarted. This is needed for the restarting time gauge
						stateTimestamps[i] = 0;
					}
				}

				transitionState(JobStatus.RESTARTING, JobStatus.CREATED);

				// if we have checkpointed state, reload it into the executions
				if (checkpointCoordinator != null) {
					checkpointCoordinator.restoreLatestCheckpointedState(getAllVertices(), false, false);
				}
			}

			scheduleForExecution();
		}
		catch (Throwable t) {
			LOG.warn("Failed to restart the job.", t);
			failGlobal(t);
		}
	}

	/**
	 * Restores the latest checkpointed state.
	 *
	 * <p>The recovery of checkpoints might block. Make sure that calls to this method don't
	 * block the job manager actor and run asynchronously.
	 *
	 * @param errorIfNoCheckpoint Fail if there is no checkpoint available
	 * @param allowNonRestoredState Allow to skip checkpoint state that cannot be mapped
	 * to the ExecutionGraph vertices (if the checkpoint contains state for a
	 * job vertex that is not part of this ExecutionGraph).
	 */
	public void restoreLatestCheckpointedState(boolean errorIfNoCheckpoint, boolean allowNonRestoredState) throws Exception {
		synchronized (progressLock) {
			if (checkpointCoordinator != null) {
				checkpointCoordinator.restoreLatestCheckpointedState(getAllVertices(), errorIfNoCheckpoint, allowNonRestoredState);
			}
		}
	}

	/**
	 * Returns the serializable {@link ArchivedExecutionConfig}.
	 *
	 * @return ArchivedExecutionConfig which may be null in case of errors
	 */
	@Override
	public ArchivedExecutionConfig getArchivedExecutionConfig() {
		// create a summary of all relevant data accessed in the web interface's JobConfigHandler
		try {
			ExecutionConfig executionConfig = jobInformation.getSerializedExecutionConfig().deserializeValue(userClassLoader);
			if (executionConfig != null) {
				return executionConfig.archive();
			}
		} catch (IOException | ClassNotFoundException e) {
			LOG.error("Couldn't create ArchivedExecutionConfig for job {} ", getJobID(), e);
		}
		return null;
	}

	@VisibleForTesting
	public CompletableFuture<JobStatus> getTerminationFuture() {
		return terminationFuture;
	}

	@VisibleForTesting
	public JobStatus waitUntilTerminal() throws InterruptedException {
		try {
			return terminationFuture.get();
		}
		catch (ExecutionException e) {
			// this should never happen
			// it would be a bug, so we  don't expect this to be handled and throw
			// an unchecked exception here
			throw new RuntimeException(e);
		}
	}

	/**
	 * Gets the failover strategy used by the execution graph to recover from failures of tasks.
	 */
	public FailoverStrategy getFailoverStrategy() {
		return this.failoverStrategy;
	}

	/**
	 * Gets the current global modification version of the ExecutionGraph.
	 * The global modification version is incremented with each global action (cancel/fail/restart)
	 * and is used to disambiguate concurrent modifications between local and global
	 * failover actions.
	 */
	long getGlobalModVersion() {
		return globalModVersion;
	}

	// ------------------------------------------------------------------------
	//  State Transitions
	// ------------------------------------------------------------------------

	private boolean transitionState(JobStatus current, JobStatus newState) {
		return transitionState(current, newState, null);
	}

	private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
		// consistency check
		if (current.isTerminalState()) {
			String message = "Job is trying to leave terminal state " + current;
			LOG.error(message);
			throw new IllegalStateException(message);
		}

		// now do the actual state transition
		if (STATE_UPDATER.compareAndSet(this, current, newState)) {
			LOG.info("Job {} ({}) switched from state {} to {}.", getJobName(), getJobID(), current, newState, error);

			stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
			notifyJobStatusChange(newState, error);
			return true;
		}
		else {
			return false;
		}
	}

	private long incrementGlobalModVersion() {
		return GLOBAL_VERSION_UPDATER.incrementAndGet(this);
	}

	private void initFailureCause(Throwable t) {
		this.failureCause = t;
		this.failureInfo = new ErrorInfo(t, System.currentTimeMillis());
	}

	// ------------------------------------------------------------------------
	//  Job Status Progress
	// ------------------------------------------------------------------------

	/**
	 * Called whenever a vertex reaches state FINISHED (completed successfully).
	 * Once all vertices are in the FINISHED state, the program is successfully done.
	 */
	void vertexFinished() {
		final int numFinished = verticesFinished.incrementAndGet();
		if (numFinished == numVerticesTotal) {
			// done :-)

			// check whether we are still in "RUNNING" and trigger the final cleanup
			if (state == JobStatus.RUNNING) {
				// we do the final cleanup in the I/O executor, because it may involve
				// some heavier work

				try {
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						ejv.getJobVertex().finalizeOnMaster(getUserClassLoader());
					}
				}
				catch (Throwable t) {
					ExceptionUtils.rethrowIfFatalError(t);
					failGlobal(new Exception("Failed to finalize execution on master", t));
					return;
				}

				// if we do not make this state transition, then a concurrent
				// cancellation or failure happened
				if (transitionState(JobStatus.RUNNING, JobStatus.FINISHED)) {
					onTerminalState(JobStatus.FINISHED);
				}
			}
		}
	}

	void vertexUnFinished() {
		verticesFinished.getAndDecrement();
	}

	/**
	 * This method is a callback during cancellation/failover and called when all tasks
	 * have reached a terminal state (cancelled/failed/finished).
	 */
	private void allVerticesInTerminalState(long expectedGlobalVersionForRestart) {
		// we are done, transition to the final state
		JobStatus current;
		while (true) {
			current = this.state;

			if (current == JobStatus.RUNNING) {
				failGlobal(new Exception("ExecutionGraph went into allVerticesInTerminalState() from RUNNING"));
			}
			else if (current == JobStatus.CANCELLING) {
				if (transitionState(current, JobStatus.CANCELED)) {
					onTerminalState(JobStatus.CANCELED);
					break;
				}
			}
			else if (current == JobStatus.FAILING) {
				if (tryRestartOrFail(expectedGlobalVersionForRestart)) {
					break;
				}
				// concurrent job status change, let's check again
			}
			else if (current == JobStatus.SUSPENDED) {
				// we've already cleaned up when entering the SUSPENDED state
				break;
			}
			else if (current.isGloballyTerminalState()) {
				LOG.warn("Job has entered globally terminal state without waiting for all " +
						"job vertices to reach final state.");
				break;
			}
			else {
				failGlobal(new Exception("ExecutionGraph went into final state from state " + current));
				break;
			}
		}
		// done transitioning the state
	}

	/**
	 * Try to restart the job. If we cannot restart the job (e.g. no more restarts allowed), then
	 * try to fail the job. This operation is only permitted if the current state is FAILING or
	 * RESTARTING.
	 *
	 * @return true if the operation could be executed; false if a concurrent job status change occurred
	 */
	private boolean tryRestartOrFail(long globalModVersionForRestart) {
		JobStatus currentState = state;

		if (currentState == JobStatus.FAILING || currentState == JobStatus.RESTARTING) {
			final Throwable failureCause = this.failureCause;

			synchronized (progressLock) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Try to restart or fail the job {} ({}) if no longer possible.", getJobName(), getJobID(), failureCause);
				} else {
					LOG.info("Try to restart or fail the job {} ({}) if no longer possible.", getJobName(), getJobID());
				}

				final boolean isFailureCauseAllowingRestart = !(failureCause instanceof SuppressRestartsException);
				final boolean isRestartStrategyAllowingRestart = restartStrategy.canRestart();
				boolean isRestartable = isFailureCauseAllowingRestart && isRestartStrategyAllowingRestart;

				if (isRestartable && transitionState(currentState, JobStatus.RESTARTING)) {
					LOG.info("Restarting the job {} ({}).", getJobName(), getJobID());

					RestartCallback restarter = new ExecutionGraphRestartCallback(this, globalModVersionForRestart);
					restartStrategy.restart(restarter, new ScheduledExecutorServiceAdapter(futureExecutor));

					return true;
				}
				else if (!isRestartable && transitionState(currentState, JobStatus.FAILED, failureCause)) {
					final String cause1 = isFailureCauseAllowingRestart ? null :
							"a type of SuppressRestartsException was thrown";
					final String cause2 = isRestartStrategyAllowingRestart ? null :
						"the restart strategy prevented it";

					LOG.info("Could not restart the job {} ({}) because {}.", getJobName(), getJobID(),
						StringUtils.concatenateWithAnd(cause1, cause2), failureCause);
					onTerminalState(JobStatus.FAILED);

					return true;
				} else {
					// we must have changed the state concurrently, thus we cannot complete this operation
					return false;
				}
			}
		} else {
			// this operation is only allowed in the state FAILING or RESTARTING
			return false;
		}
	}

	private void onTerminalState(JobStatus status) {
		try {
			CheckpointCoordinator coord = this.checkpointCoordinator;
			this.checkpointCoordinator = null;
			if (coord != null) {
				coord.shutdown(status);
			}
		}
		catch (Exception e) {
			LOG.error("Error while cleaning up after execution", e);
		}
		finally {
			terminationFuture.complete(status);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Callbacks and Callback Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Updates the state of one of the ExecutionVertex's Execution attempts.
	 * If the new status if "FINISHED", this also updates the accumulators.
	 * 
	 * @param state The state update.
	 * @return True, if the task update was properly applied, false, if the execution attempt was not found.
	 */
	public boolean updateState(TaskExecutionState state) {
		final Execution attempt = currentExecutions.get(state.getID());

		if (attempt != null) {
			try {
				Map<String, Accumulator<?, ?>> accumulators;

				switch (state.getExecutionState()) {
					case RUNNING:
						return attempt.switchToRunning();

					case FINISHED:
						// this deserialization is exception-free
						accumulators = deserializeAccumulators(state);
						attempt.markFinished(accumulators, state.getIOMetrics());
						return true;

					case CANCELED:
						// this deserialization is exception-free
						accumulators = deserializeAccumulators(state);
						attempt.cancelingComplete(accumulators, state.getIOMetrics());
						return true;

					case FAILED:
						// this deserialization is exception-free
						accumulators = deserializeAccumulators(state);
						attempt.markFailed(state.getError(userClassLoader), accumulators, state.getIOMetrics());
						return true;

					default:
						// we mark as failed and return false, which triggers the TaskManager
						// to remove the task
						attempt.fail(new Exception("TaskManager sent illegal state update: " + state.getExecutionState()));
						return false;
				}
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

				// failures during updates leave the ExecutionGraph inconsistent
				failGlobal(t);
				return false;
			}
		}
		else {
			return false;
		}
	}

	/**
	 * Deserializes accumulators from a task state update.
	 *
	 * <p>This method never throws an exception!
	 *
	 * @param state The task execution state from which to deserialize the accumulators.
	 * @return The deserialized accumulators, of null, if there are no accumulators or an error occurred.
	 */
	private Map<String, Accumulator<?, ?>> deserializeAccumulators(TaskExecutionState state) {
		AccumulatorSnapshot serializedAccumulators = state.getAccumulators();

		if (serializedAccumulators != null) {
			try {
				return serializedAccumulators.deserializeUserAccumulators(userClassLoader);
			}
			catch (Throwable t) {
				// we catch Throwable here to include all form of linking errors that may
				// occur if user classes are missing in the classpath
				LOG.error("Failed to deserialize final accumulator results.", t);
			}
		}
		return null;
	}

	/**
	 * Schedule or updates consumers of the given result partition.
	 *
	 * @param partitionId specifying the result partition whose consumer shall be scheduled or updated
	 * @throws ExecutionGraphException if the schedule or update consumers operation could not be executed
	 */
	public void scheduleOrUpdateConsumers(ResultPartitionID partitionId) throws ExecutionGraphException {

		final Execution execution = currentExecutions.get(partitionId.getProducerId());

		if (execution == null) {
			throw new ExecutionGraphException("Cannot find execution for execution Id " +
				partitionId.getPartitionId() + '.');
		}
		else if (execution.getVertex() == null){
			throw new ExecutionGraphException("Execution with execution Id " +
				partitionId.getPartitionId() + " has no vertex assigned.");
		} else {
			execution.getVertex().scheduleOrUpdateConsumers(partitionId);
		}
	}

	public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
		return Collections.unmodifiableMap(currentExecutions);
	}

	void registerExecution(Execution exec) {
		Execution previous = currentExecutions.putIfAbsent(exec.getAttemptId(), exec);
		if (previous != null) {
			failGlobal(new Exception("Trying to register execution " + exec + " for already used ID " + exec.getAttemptId()));
		}
	}

	void deregisterExecution(Execution exec) {
		Execution contained = currentExecutions.remove(exec.getAttemptId());

		if (contained != null && contained != exec) {
			failGlobal(new Exception("De-registering execution " + exec + " failed. Found for same ID execution " + contained));
		}
	}

	/**
	 * Updates the accumulators during the runtime of a job. Final accumulator results are transferred
	 * through the UpdateTaskExecutionState message.
	 * @param accumulatorSnapshot The serialized flink and user-defined accumulators
	 */
	public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
		Map<String, Accumulator<?, ?>> userAccumulators;
		try {
			userAccumulators = accumulatorSnapshot.deserializeUserAccumulators(userClassLoader);

			ExecutionAttemptID execID = accumulatorSnapshot.getExecutionAttemptID();
			Execution execution = currentExecutions.get(execID);
			if (execution != null) {
				execution.setAccumulators(userAccumulators);
			} else {
				LOG.debug("Received accumulator result for unknown execution {}.", execID);
			}
		} catch (Exception e) {
			LOG.error("Cannot update accumulators for job {}.", getJobID(), e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Listeners & Observers
	// --------------------------------------------------------------------------------------------

	public void registerJobStatusListener(JobStatusListener listener) {
		if (listener != null) {
			jobStatusListeners.add(listener);
		}
	}

	public void registerExecutionListener(ExecutionStatusListener listener) {
		if (listener != null) {
			executionListeners.add(listener);
		}
	}

	private void notifyJobStatusChange(JobStatus newState, Throwable error) {
		if (jobStatusListeners.size() > 0) {
			final long timestamp = System.currentTimeMillis();
			final Throwable serializedError = error == null ? null : new SerializedThrowable(error);

			for (JobStatusListener listener : jobStatusListeners) {
				try {
					listener.jobStatusChanges(getJobID(), newState, timestamp, serializedError);
				} catch (Throwable t) {
					LOG.warn("Error while notifying JobStatusListener", t);
				}
			}
		}
	}

	void notifyExecutionChange(
			final Execution execution,
			final ExecutionState newExecutionState,
			final Throwable error) {

		if (executionListeners.size() > 0) {
			final ExecutionJobVertex vertex = execution.getVertex().getJobVertex();
			final String message = error == null ? null : ExceptionUtils.stringifyException(error);
			final long timestamp = System.currentTimeMillis();

			for (ExecutionStatusListener listener : executionListeners) {
				try {
					listener.executionStatusChanged(
							getJobID(), vertex.getJobVertexId(), vertex.getJobVertex().getName(),
							vertex.getParallelism(), execution.getParallelSubtaskIndex(),
							execution.getAttemptId(), newExecutionState, timestamp, message);
				} catch (Throwable t) {
					LOG.warn("Error while notifying ExecutionStatusListener", t);
				}
			}
		}

		// see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			final Throwable ex = error != null ? error : new FlinkException("Unknown Error (missing cause)");
			long timestamp = execution.getStateTimestamp(ExecutionState.FAILED);

			// by filtering out late failure calls, we can save some work in
			// avoiding redundant local failover
			if (execution.getGlobalModVersion() == globalModVersion) {
				try {
					failoverStrategy.onTaskFailure(execution, ex);

					// fail all checkpoints which the failed task has not yet acknowledged
					if (checkpointCoordinator != null) {
						checkpointCoordinator.failUnacknowledgedPendingCheckpointsFor(execution.getAttemptId(), ex);
					}
				}
				catch (Throwable t) {
					// bug in the failover strategy - fall back to global failover
					LOG.warn("Error in failover strategy - falling back to global restart", t);
					failGlobal(ex);
				}
			}
		}
	}

	@Override
	public ArchivedExecutionGraph archive() {
		Map<JobVertexID, ArchivedExecutionJobVertex> archivedTasks = new HashMap<>(verticesInCreationOrder.size());
		List<ArchivedExecutionJobVertex> archivedVerticesInCreationOrder = new ArrayList<>(verticesInCreationOrder.size());

		for (ExecutionJobVertex task : verticesInCreationOrder) {
			ArchivedExecutionJobVertex archivedTask = task.archive();
			archivedVerticesInCreationOrder.add(archivedTask);
			archivedTasks.put(task.getJobVertexId(), archivedTask);
		}

		Map<String, SerializedValue<Object>> serializedUserAccumulators;
		try {
			serializedUserAccumulators = getAccumulatorsSerialized();
		} catch (Exception e) {
			LOG.warn("Error occurred while archiving user accumulators.", e);
			serializedUserAccumulators = Collections.emptyMap();
		}

		return new ArchivedExecutionGraph(
			getJobID(),
			getJobName(),
			archivedTasks,
			archivedVerticesInCreationOrder,
			stateTimestamps,
			getState(),
			failureInfo,
			getJsonPlan(),
			getAccumulatorResultsStringified(),
			serializedUserAccumulators,
			getArchivedExecutionConfig(),
			isStoppable(),
			getCheckpointCoordinatorConfiguration(),
			getCheckpointStatsSnapshot());
	}
}
