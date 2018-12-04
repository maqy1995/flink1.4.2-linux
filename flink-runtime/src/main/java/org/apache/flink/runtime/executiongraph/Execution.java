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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.PartialInputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.runtime.execution.ExecutionState.CANCELED;
import static org.apache.flink.runtime.execution.ExecutionState.CANCELING;
import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FAILED;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.runtime.execution.ExecutionState.SCHEDULED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single execution
 * of that vertex and the resources.
 * 
 * <h2>Lock free state transitions</h2>
 * 
 * In several points of the code, we need to deal with possible concurrent state changes and actions.
 * For example, while the call to deploy a task (send it to the TaskManager) happens, the task gets cancelled.
 * 
 * <p>We could lock the entire portion of the code (decision to deploy, deploy, set state to running) such that
 * it is guaranteed that any "cancel command" will only pick up after deployment is done and that the "cancel
 * command" call will never overtake the deploying call.
 * 
 * <p>This blocks the threads big time, because the remote calls may take long. Depending of their locking behavior, it
 * may even result in distributed deadlocks (unless carefully avoided). We therefore use atomic state updates and
 * occasional double-checking to ensure that the state after a completed call is as expected, and trigger correcting
 * actions if it is not. Many actions are also idempotent (like canceling).
 */
public class Execution implements AccessExecution, Archiveable<ArchivedExecution> {

	private static final AtomicReferenceFieldUpdater<Execution, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Execution.class, ExecutionState.class, "state");

	private static final AtomicReferenceFieldUpdater<Execution, SimpleSlot> ASSIGNED_SLOT_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
		Execution.class,
		SimpleSlot.class,
		"assignedResource");

	private static final Logger LOG = ExecutionGraph.LOG;

	private static final int NUM_CANCEL_CALL_TRIES = 3;

	private static final int NUM_STOP_CALL_TRIES = 3;

	// --------------------------------------------------------------------------------------------

	/** The executor which is used to execute futures. */
	private final Executor executor;

	/** The execution vertex whose task this execution executes */ 
	private final ExecutionVertex vertex;

	/** The unique ID marking the specific execution instant of the task */
	private final ExecutionAttemptID attemptId;

	/** Gets the global modification version of the execution graph when this execution was created.
	 * This version is bumped in the ExecutionGraph whenever a global failover happens. It is used
	 * to resolve conflicts between concurrent modification by global and local failover actions. */
	private final long globalModVersion;

	/** The timestamps when state transitions occurred, indexed by {@link ExecutionState#ordinal()} */ 
	private final long[] stateTimestamps;

	private final int attemptNumber;

	private final Time timeout;

	private final ConcurrentLinkedQueue<PartialInputChannelDeploymentDescriptor> partialInputChannelDeploymentDescriptors;

	/** A future that completes once the Execution reaches a terminal ExecutionState */
	private final CompletableFuture<ExecutionState> terminationFuture;

	private final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture;

	private volatile ExecutionState state = CREATED;

	private volatile SimpleSlot assignedResource;

	private volatile Throwable failureCause;          // once assigned, never changes

	/** The handle to the state that the task gets on restore */
	private volatile TaskStateSnapshot taskState;

	// ------------------------ Accumulators & Metrics ------------------------

	/** Lock for updating the accumulators atomically.
	 * Prevents final accumulators to be overwritten by partial accumulators on a late heartbeat */
	private final Object accumulatorLock = new Object();

	/* Continuously updated map of user-defined accumulators */
	private volatile Map<String, Accumulator<?, ?>> userAccumulators;

	private volatile IOMetrics ioMetrics;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new Execution attempt.
	 * 
	 * @param executor
	 *             The executor used to dispatch callbacks from futures and asynchronous RPC calls.
	 * @param vertex
	 *             The execution vertex to which this Execution belongs
	 * @param attemptNumber
	 *             The execution attempt number.
	 * @param globalModVersion
	 *             The global modification version of the execution graph when this execution was created
	 * @param startTimestamp
	 *             The timestamp that marks the creation of this Execution
	 * @param timeout
	 *             The timeout for RPC calls like deploy/cancel/stop.
	 */
	public Execution(
			Executor executor,
			ExecutionVertex vertex,
			int attemptNumber,
			long globalModVersion,
			long startTimestamp,
			Time timeout) {

		this.executor = checkNotNull(executor);
		this.vertex = checkNotNull(vertex);
		this.attemptId = new ExecutionAttemptID();
		this.timeout = checkNotNull(timeout);

		this.globalModVersion = globalModVersion;
		this.attemptNumber = attemptNumber;

		this.stateTimestamps = new long[ExecutionState.values().length];
		markTimestamp(ExecutionState.CREATED, startTimestamp);

		this.partialInputChannelDeploymentDescriptors = new ConcurrentLinkedQueue<>();
		this.terminationFuture = new CompletableFuture<>();
		this.taskManagerLocationFuture = new CompletableFuture<>();

		this.assignedResource = null;
	}

	// --------------------------------------------------------------------------------------------
	//   Properties
	// --------------------------------------------------------------------------------------------

	public ExecutionVertex getVertex() {
		return vertex;
	}

	@Override
	public ExecutionAttemptID getAttemptId() {
		return attemptId;
	}

	@Override
	public int getAttemptNumber() {
		return attemptNumber;
	}

	@Override
	public ExecutionState getState() {
		return state;
	}

	/**
	 * Gets the global modification version of the execution graph when this execution was created.
	 * 
	 * <p>This version is bumped in the ExecutionGraph whenever a global failover happens. It is used
	 * to resolve conflicts between concurrent modification by global and local failover actions.
	 */
	public long getGlobalModVersion() {
		return globalModVersion;
	}

	public CompletableFuture<TaskManagerLocation> getTaskManagerLocationFuture() {
		return taskManagerLocationFuture;
	}

	public SimpleSlot getAssignedResource() {
		return assignedResource;
	}

	/**
	 * Tries to assign the given slot to the execution. The assignment works only if the
	 * Execution is in state SCHEDULED. Returns true, if the resource could be assigned.
	 *
	 * @param slot to assign to this execution
	 * @return true if the slot could be assigned to the execution, otherwise false
	 */
	@VisibleForTesting
	boolean tryAssignResource(final SimpleSlot slot) {
		checkNotNull(slot);

		// only allow to set the assigned resource in state SCHEDULED or CREATED
		// note: we also accept resource assignment when being in state CREATED for testing purposes
		if (state == SCHEDULED || state == CREATED) {
			if (ASSIGNED_SLOT_UPDATER.compareAndSet(this, null, slot)) {
				// check for concurrent modification (e.g. cancelling call)
				if (state == SCHEDULED || state == CREATED) {
					checkState(!taskManagerLocationFuture.isDone(), "The TaskManagerLocationFuture should not be set if we haven't assigned a resource yet.");
					taskManagerLocationFuture.complete(slot.getTaskManagerLocation());

					return true;
				} else {
					// free assigned resource and return false
					ASSIGNED_SLOT_UPDATER.set(this, null);
					return false;
				}
			} else {
				// the slot already has another slot assigned
				return false;
			}
		} else {
			// do not allow resource assignment if we are not in state SCHEDULED
			return false;
		}
	}

	@Override
	public TaskManagerLocation getAssignedResourceLocation() {
		// returns non-null only when a location is already assigned
		final SimpleSlot currentAssignedResource = assignedResource;
		return currentAssignedResource != null ? currentAssignedResource.getTaskManagerLocation() : null;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	@Override
	public String getFailureCauseAsString() {
		return ExceptionUtils.stringifyException(getFailureCause());
	}

	@Override
	public long[] getStateTimestamps() {
		return stateTimestamps;
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return this.stateTimestamps[state.ordinal()];
	}

	public boolean isFinished() {
		return state.isTerminal();
	}

	public TaskStateSnapshot getTaskStateSnapshot() {
		return taskState;
	}

	/**
	 * Sets the initial state for the execution. The serialized state is then shipped via the
	 * {@link TaskDeploymentDescriptor} to the TaskManagers.
	 *
	 * @param checkpointStateHandles all checkpointed operator state
	 */
	public void setInitialState(TaskStateSnapshot checkpointStateHandles) {
		checkState(state == CREATED, "Can only assign operator state when execution attempt is in CREATED");
		this.taskState = checkpointStateHandles;
	}

	/**
	 * Gets a future that completes once the task execution reaches a terminal state.
	 * The future will be completed with specific state that the execution reached.
	 *
	 * @return A future for the execution's termination
	 */
	public CompletableFuture<ExecutionState> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public boolean scheduleForExecution() {
		SlotProvider resourceProvider = getVertex().getExecutionGraph().getSlotProvider();
		boolean allowQueued = getVertex().getExecutionGraph().isQueuedSchedulingAllowed();
		return scheduleForExecution(resourceProvider, allowQueued, LocationPreferenceConstraint.ANY);
	}

	/**
	 * NOTE: This method only throws exceptions if it is in an illegal state to be scheduled, or if the tasks needs
	 *       to be scheduled immediately and no resource is available. If the task is accepted by the schedule, any
	 *       error sets the vertex state to failed and triggers the recovery logic.
	 * 
	 * @param slotProvider The slot provider to use to allocate slot for this execution attempt.
	 * @param queued Flag to indicate whether the scheduler may queue this task if it cannot
	 *               immediately deploy it.
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * 
	 * @throws IllegalStateException Thrown, if the vertex is not in CREATED state, which is the only state that permits scheduling.
	 */
	public boolean scheduleForExecution(
		SlotProvider slotProvider,
		boolean queued,
		LocationPreferenceConstraint locationPreferenceConstraint) {
		try {
			final CompletableFuture<Execution> allocationFuture = allocateAndAssignSlotForExecution(
				slotProvider,
				queued,
				locationPreferenceConstraint);

			// IMPORTANT: We have to use the synchronous handle operation (direct executor) here so
			// that we directly deploy the tasks if the slot allocation future is completed. This is
			// necessary for immediate deployment.
			final CompletableFuture<Void> deploymentFuture = allocationFuture.handle(
				(Execution ignored, Throwable throwable) ->  {
					if (throwable != null) {
						markFailed(ExceptionUtils.stripCompletionException(throwable));
					}
					else {
						try {
							deploy();
						} catch (Throwable t) {
							markFailed(ExceptionUtils.stripCompletionException(t));
						}
					}
					return null;
				}
			);

			// if tasks have to scheduled immediately check that the task has been deployed
			if (!queued && !deploymentFuture.isDone()) {
				markFailed(new IllegalArgumentException("The slot allocation future has not been completed yet."));
			}

			return true;
		}
		catch (IllegalExecutionStateException e) {
			return false;
		}
	}

	/**
	 * Allocates and assigns a slot obtained from the slot provider to the execution.
	 *
	 * @param slotProvider to obtain a new slot from
	 * @param queued if the allocation can be queued
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @return Future which is completed with this execution once the slot has been assigned
	 * 			or with an exception if an error occurred.
	 * @throws IllegalExecutionStateException if this method has been called while not being in the CREATED state
	 *
	 * a、将状态从’CREATED’成功转换成’SCHEDULED’;
	 * b、根据LocationPreferenceConstraint的设置,为这个Execution指定优先分配槽位所在的TaskManager;
	 * c、基于上述步骤获取的偏好位置,进行slot分配;
	 * d、在slot分配成功后,将slot设定给当前Execution,如果设定成功,则返回相应的slot,否则是否slot,然后抛出异常。
	 *
	 * 其中LocationPreferenceConstraint有两种取值:
	 *
	 * a、ALL —— 需要确认其所有的输入都已经分配好slot,然后基于其输入所在的TaskManager,作为其偏好位置集合;
	 * b、ANY —— 只考虑那些slot已经分配好的输入所在的TaskManager,作为偏好位置集合;
	 *
	 * 某个Execution的偏好位置的计算逻辑,是先由其对应的ExecutionVertex基于所有输入,获取偏好位置集合,
	 * 然后根据LocationPreferenceConstraint的策略不同,删选出一个子集,作为这个Execution的偏好位置集合。
	 */
	public CompletableFuture<Execution> allocateAndAssignSlotForExecution(
			SlotProvider slotProvider,
			boolean queued,
			LocationPreferenceConstraint locationPreferenceConstraint) throws IllegalExecutionStateException {

		checkNotNull(slotProvider);

		//获取在构建JobVertex时已经赋值好的SlotSharingGroup实例和CoLocationConstraint实例,如果有的话
		final SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
		final CoLocationConstraint locationConstraint = vertex.getLocationConstraint();

		// sanity check
		//位置约束不为null, 而共享组为null, 这种情况是不可能出现的, 出现了肯定就是异常了
		if (locationConstraint != null && sharingGroup == null) {
			throw new IllegalStateException(
					"Trying to schedule with co-location constraint but without slot sharing allowed.");
		}

		// this method only works if the execution is in the state 'CREATED'
		//只有状态是 'CREATED' 时, 这个方法才能正常工作
		if (transitionState(CREATED, SCHEDULED)) {

			//ScheduleUnit 实例就是在这里构造出来的
			ScheduledUnit toSchedule = locationConstraint == null ?
					new ScheduledUnit(this, sharingGroup) :
					new ScheduledUnit(this, sharingGroup, locationConstraint);

			// calculate the preferred locations
			//获取当前任务分配槽位所在节点的"偏好位置集合",也就是分配时,优先考虑分配在这些节点上
			final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture = calculatePreferredLocations(locationPreferenceConstraint);

			return preferredLocationsFuture
				.thenCompose(
					(Collection<TaskManagerLocation> preferredLocations) ->
						slotProvider.allocateSlot( //在获取输入节点的位置之后,将其作为偏好位置集合,基于这些偏好位置,申请分配一个slot
							toSchedule,
							queued,
							preferredLocations))
				.thenApply(
					(SimpleSlot slot) -> {
						if (tryAssignResource(slot)) {
							return this;  //如果slot分配成功,则返回这个future
						} else {
							// release the slot
							// 释放slot
							slot.releaseSlot();

							throw new CompletionException(new FlinkException("Could not assign slot " + slot + " to execution " + this + " because it has already been assigned "));
						}
					});
		}
		else {
			// call race, already deployed, or already done
			throw new IllegalExecutionStateException(this, CREATED, state);
		}
	}

	/**
	 * Deploys the execution to the previously assigned resource.
	 *
	 * @throws JobException if the execution cannot be deployed to the assigned resource
	 */
	public void deploy() throws JobException {
		final SimpleSlot slot  = assignedResource;

		checkNotNull(slot, "In order to deploy the execution we first have to assign a resource via tryAssignResource.");

		// Check if the TaskManager died in the meantime
		// This only speeds up the response to TaskManagers failing concurrently to deployments.
		// The more general check is the timeout of the deployment call
		// 检查slot是否alive
		if (!slot.isAlive()) {
			throw new JobException("Target slot (TaskManager) for deployment is no longer alive.");
		}

		// make sure exactly one deployment call happens from the correct state
		// note: the transition from CREATED to DEPLOYING is for testing purposes only
		//确保在正确的状态的情况下进行部署调用 * 注意:从 CREATED to DEPLOYING 只是用来测试的
		ExecutionState previous = this.state;
		if (previous == SCHEDULED || previous == CREATED) {
			if (!transitionState(previous, DEPLOYING)) {
				// race condition, someone else beat us to the deploying call.
				// this should actually not happen and indicates a race somewhere else
				// 竞态条件,有人在部署调用上击中我们了(其实就是冲突了) * 这个在真实情况下不该发生,如果发生,则说明有地方发生冲突了
				throw new IllegalStateException("Cannot deploy task: Concurrent deployment call race.");
			}
		}
		else {
			// vertex may have been cancelled, or it was already scheduled
			// 能已经被取消了,或者已经被调度了
			throw new IllegalStateException("The vertex must be in CREATED or SCHEDULED state to be deployed. Found state " + previous);
		}

		try {
			// good, we are allowed to deploy
			if (!slot.setExecutedVertex(this)) {
				throw new JobException("Could not assign the ExecutionVertex to the slot " + slot);
			}

			// race double check, did we fail/cancel and do we need to release the slot?
			//双重校验,是我们 失败/取消 ? 我们需要释放这个slot?
			if (this.state != DEPLOYING) {
				slot.releaseSlot();
				return;
			}

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Deploying %s (attempt #%d) to %s", vertex.getTaskNameWithSubtaskIndex(),
						attemptNumber, getAssignedResourceLocation().getHostname()));
			}

			final TaskDeploymentDescriptor deployment = vertex.createDeploymentDescriptor(
				attemptId,
				slot,
				taskState,
				attemptNumber);

			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			//这里就是将task提交到{@code TaskManager}的地方
			final CompletableFuture<Acknowledge> submitResultFuture = taskManagerGateway.submitTask(deployment, timeout);

			//根据提交结果进行处理,如果提交失败,则进行fail处理
			submitResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					// only respond to the failure case
					if (failure != null) {
						if (failure instanceof TimeoutException) {
							String taskname = vertex.getTaskNameWithSubtaskIndex() + " (" + attemptId + ')';

							markFailed(new Exception(
								"Cannot deploy task " + taskname + " - TaskManager (" + getAssignedResourceLocation()
									+ ") not responding after a timeout of " + timeout, failure));
						} else {
							markFailed(failure);
						}
					}
				},
				executor);
		}
		catch (Throwable t) {
			markFailed(t);
			ExceptionUtils.rethrow(t);
		}
	}

	/**
	 * Sends stop RPC call.
	 */
	public void stop() {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			CompletableFuture<Acknowledge> stopResultFuture = FutureUtils.retry(
				() -> taskManagerGateway.stopTask(attemptId, timeout),
				NUM_STOP_CALL_TRIES,
				executor);

			stopResultFuture.exceptionally(
				failure -> {
					LOG.info("Stopping task was not successful.", failure);
					return null;
				});
		}
	}

	public void cancel() {
		// depending on the previous state, we go directly to cancelled (no cancel call necessary)
		// -- or to canceling (cancel call needs to be sent to the task manager)

		// because of several possibly previous states, we need to again loop until we make a
		// successful atomic state transition
		while (true) {

			ExecutionState current = this.state;

			if (current == CANCELING || current == CANCELED) {
				// already taken care of, no need to cancel again
				return;
			}

			// these two are the common cases where we need to send a cancel call
			else if (current == RUNNING || current == DEPLOYING) {
				// try to transition to canceling, if successful, send the cancel call
				if (transitionState(current, CANCELING)) {
					sendCancelRpcCall();
					return;
				}
				// else: fall through the loop
			}

			else if (current == FINISHED || current == FAILED) {
				// nothing to do any more. finished failed before it could be cancelled.
				// in any case, the task is removed from the TaskManager already
				sendFailIntermediateResultPartitionsRpcCall();

				return;
			}
			else if (current == CREATED || current == SCHEDULED) {
				// from here, we can directly switch to cancelled, because no task has been deployed
				if (transitionState(current, CANCELED)) {

					// we skip the canceling state. set the timestamp, for a consistent appearance
					markTimestamp(CANCELING, getStateTimestamp(CANCELED));

					// cancel the future in order to fail depending scheduling operations
					taskManagerLocationFuture.cancel(false);

					try {
						vertex.getExecutionGraph().deregisterExecution(this);

						final SimpleSlot slot = assignedResource;

						if (slot != null) {
							slot.releaseSlot();
						}
					}
					finally {
						vertex.executionCanceled(this);
						terminationFuture.complete(CANCELED);
					}
					return;
				}
				// else: fall through the loop
			}
			else {
				throw new IllegalStateException(current.name());
			}
		}
	}

	void scheduleOrUpdateConsumers(List<List<ExecutionEdge>> allConsumers) {
		final int numConsumers = allConsumers.size();

		if (numConsumers > 1) {
			fail(new IllegalStateException("Currently, only a single consumer group per partition is supported."));
		}
		else if (numConsumers == 0) {
			return;
		}

		for (ExecutionEdge edge : allConsumers.get(0)) {
			final ExecutionVertex consumerVertex = edge.getTarget();

			final Execution consumer = consumerVertex.getCurrentExecutionAttempt();
			final ExecutionState consumerState = consumer.getState();

			final IntermediateResultPartition partition = edge.getSource();

			// ----------------------------------------------------------------
			// Consumer is created => try to deploy and cache input channel
			// descriptors if there is a deployment race
			// ----------------------------------------------------------------
			if (consumerState == CREATED) {
				final Execution partitionExecution = partition.getProducer()
						.getCurrentExecutionAttempt();

				consumerVertex.cachePartitionInfo(PartialInputChannelDeploymentDescriptor.fromEdge(
						partition, partitionExecution));

				// When deploying a consuming task, its task deployment descriptor will contain all
				// deployment information available at the respective time. It is possible that some
				// of the partitions to be consumed have not been created yet. These are updated
				// runtime via the update messages.
				//
				// TODO The current approach may send many update messages even though the consuming
				// task has already been deployed with all necessary information. We have to check
				// whether this is a problem and fix it, if it is.
				CompletableFuture.supplyAsync(
					() -> {
						try {
							consumerVertex.scheduleForExecution(
								consumerVertex.getExecutionGraph().getSlotProvider(),
								consumerVertex.getExecutionGraph().isQueuedSchedulingAllowed(),
								LocationPreferenceConstraint.ANY); // there must be at least one known location
						} catch (Throwable t) {
							consumerVertex.fail(new IllegalStateException("Could not schedule consumer " +
									"vertex " + consumerVertex, t));
						}

						return null;
					},
					executor);

				// double check to resolve race conditions
				if(consumerVertex.getExecutionState() == RUNNING){
					consumerVertex.sendPartitionInfos();
				}
			}
			// ----------------------------------------------------------------
			// Consumer is running => send update message now
			// ----------------------------------------------------------------
			else {
				if (consumerState == RUNNING) {
					final SimpleSlot consumerSlot = consumer.getAssignedResource();

					if (consumerSlot == null) {
						// The consumer has been reset concurrently
						continue;
					}

					final TaskManagerLocation partitionTaskManagerLocation = partition.getProducer()
							.getCurrentAssignedResource().getTaskManagerLocation();
					final ResourceID partitionTaskManager = partitionTaskManagerLocation.getResourceID();
					
					final ResourceID consumerTaskManager = consumerSlot.getTaskManagerID();

					final ResultPartitionID partitionId = new ResultPartitionID(partition.getPartitionId(), attemptId);
					

					final ResultPartitionLocation partitionLocation;

					if (consumerTaskManager.equals(partitionTaskManager)) {
						// Consuming task is deployed to the same instance as the partition => local
						partitionLocation = ResultPartitionLocation.createLocal();
					}
					else {
						// Different instances => remote
						final ConnectionID connectionId = new ConnectionID(
								partitionTaskManagerLocation,
								partition.getIntermediateResult().getConnectionIndex());

						partitionLocation = ResultPartitionLocation.createRemote(connectionId);
					}

					final InputChannelDeploymentDescriptor descriptor = new InputChannelDeploymentDescriptor(
							partitionId, partitionLocation);

					consumer.sendUpdatePartitionInfoRpcCall(
						Collections.singleton(
							new PartitionInfo(
								partition.getIntermediateResult().getId(),
								descriptor)));
				}
				// ----------------------------------------------------------------
				// Consumer is scheduled or deploying => cache input channel
				// deployment descriptors and send update message later
				// ----------------------------------------------------------------
				else if (consumerState == SCHEDULED || consumerState == DEPLOYING) {
					final Execution partitionExecution = partition.getProducer()
							.getCurrentExecutionAttempt();

					consumerVertex.cachePartitionInfo(PartialInputChannelDeploymentDescriptor
							.fromEdge(partition, partitionExecution));

					// double check to resolve race conditions
					if (consumerVertex.getExecutionState() == RUNNING) {
						consumerVertex.sendPartitionInfos();
					}
				}
			}
		}
	}

	/**
	 * This method fails the vertex due to an external condition. The task will move to state FAILED.
	 * If the task was in state RUNNING or DEPLOYING before, it will send a cancel call to the TaskManager.
	 *
	 * @param t The exception that caused the task to fail.
	 */
	public void fail(Throwable t) {
		processFail(t, false);
	}

	/**
	 * Request a stack trace sample from the task of this execution.
	 *
	 * @param sampleId of the stack trace sample
	 * @param numSamples the sample should contain
	 * @param delayBetweenSamples to wait
	 * @param maxStrackTraceDepth of the samples
	 * @param timeout until the request times out
	 * @return Future stack trace sample response
	 */
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStrackTraceDepth,
			Time timeout) {

		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			return taskManagerGateway.requestStackTraceSample(
				attemptId,
				sampleId,
				numSamples,
				delayBetweenSamples,
				maxStrackTraceDepth,
				timeout);
		} else {
			return FutureUtils.completedExceptionally(new Exception("The execution has no slot assigned."));
		}
	}

	/**
	 * Notify the task of this execution about a completed checkpoint.
	 *
	 * @param checkpointId of the completed checkpoint
	 * @param timestamp of the completed checkpoint
	 */
	public void notifyCheckpointComplete(long checkpointId, long timestamp) {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			taskManagerGateway.notifyCheckpointComplete(attemptId, getVertex().getJobId(), checkpointId, timestamp);
		} else {
			LOG.debug("The execution has no slot assigned. This indicates that the execution is " +
				"no longer running.");
		}
	}

	/**
	 * Trigger a new checkpoint on the task of this execution.
	 *
	 * @param checkpointId of th checkpoint to trigger
	 * @param timestamp of the checkpoint to trigger
	 * @param checkpointOptions of the checkpoint to trigger
	 */
	public void triggerCheckpoint(long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions);
		} else {
			LOG.debug("The execution has no slot assigned. This indicates that the execution is " +
				"no longer running.");
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Callbacks
	// --------------------------------------------------------------------------------------------

	/**
	 * This method marks the task as failed, but will make no attempt to remove task execution from the task manager.
	 * It is intended for cases where the task is known not to be running, or then the TaskManager reports failure
	 * (in which case it has already removed the task).
	 *
	 * @param t The exception that caused the task to fail.
	 */
	void markFailed(Throwable t) {
		processFail(t, true);
	}

	void markFailed(Throwable t, Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {
		processFail(t, true, userAccumulators, metrics);
	}

	void markFinished() {
		markFinished(null, null);
	}

	void markFinished(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

		// this call usually comes during RUNNING, but may also come while still in deploying (very fast tasks!)
		while (true) {
			ExecutionState current = this.state;

			if (current == RUNNING || current == DEPLOYING) {

				if (transitionState(current, FINISHED)) {
					try {
						for (IntermediateResultPartition finishedPartition
								: getVertex().finishAllBlockingPartitions()) {

							IntermediateResultPartition[] allPartitions = finishedPartition
									.getIntermediateResult().getPartitions();

							for (IntermediateResultPartition partition : allPartitions) {
								scheduleOrUpdateConsumers(partition.getConsumers());
							}
						}

						updateAccumulatorsAndMetrics(userAccumulators, metrics);

						final SimpleSlot slot = assignedResource;

						if (slot != null) {
							slot.releaseSlot();
						}

						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionFinished(this);
						terminationFuture.complete(FINISHED);
					}
					return;
				}
			}
			else if (current == CANCELING) {
				// we sent a cancel call, and the task manager finished before it arrived. We
				// will never get a CANCELED call back from the job manager
				cancelingComplete(userAccumulators, metrics);
				return;
			}
			else if (current == CANCELED || current == FAILED) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Task FINISHED, but concurrently went to state " + state);
				}
				return;
			}
			else {
				// this should not happen, we need to fail this
				markFailed(new Exception("Vertex received FINISHED message while being in state " + state));
				return;
			}
		}
	}

	void cancelingComplete() {
		cancelingComplete(null, null);
	}
	
	void cancelingComplete(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

		// the taskmanagers can themselves cancel tasks without an external trigger, if they find that the
		// network stack is canceled (for example by a failing / canceling receiver or sender
		// this is an artifact of the old network runtime, but for now we need to support task transitions
		// from running directly to canceled

		while (true) {
			ExecutionState current = this.state;

			if (current == CANCELED) {
				return;
			}
			else if (current == CANCELING || current == RUNNING || current == DEPLOYING) {

				updateAccumulatorsAndMetrics(userAccumulators, metrics);

				if (transitionState(current, CANCELED)) {
					try {
						final SimpleSlot slot = assignedResource;

						if (slot != null) {
							slot.releaseSlot();
						}

						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionCanceled(this);
						terminationFuture.complete(CANCELED);
					}
					return;
				}

				// else fall through the loop
			}
			else {
				// failing in the meantime may happen and is no problem.
				// anything else is a serious problem !!!
				if (current != FAILED) {
					String message = String.format("Asynchronous race: Found %s in state %s after successful cancel call.", vertex.getTaskNameWithSubtaskIndex(), state);
					LOG.error(message);
					vertex.getExecutionGraph().failGlobal(new Exception(message));
				}
				return;
			}
		}
	}

	void cachePartitionInfo(PartialInputChannelDeploymentDescriptor partitionInfo) {
		partialInputChannelDeploymentDescriptors.add(partitionInfo);
	}

	void sendPartitionInfos() {
		// check if the ExecutionVertex has already been archived and thus cleared the
		// partial partition infos queue
		if (partialInputChannelDeploymentDescriptors != null && !partialInputChannelDeploymentDescriptors.isEmpty()) {

			PartialInputChannelDeploymentDescriptor partialInputChannelDeploymentDescriptor;

			List<PartitionInfo> partitionInfos = new ArrayList<>(partialInputChannelDeploymentDescriptors.size());

			while ((partialInputChannelDeploymentDescriptor = partialInputChannelDeploymentDescriptors.poll()) != null) {
				partitionInfos.add(
					new PartitionInfo(
						partialInputChannelDeploymentDescriptor.getResultId(),
						partialInputChannelDeploymentDescriptor.createInputChannelDeploymentDescriptor(this)));
			}

			sendUpdatePartitionInfoRpcCall(partitionInfos);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Actions
	// --------------------------------------------------------------------------------------------

	private boolean processFail(Throwable t, boolean isCallback) {
		return processFail(t, isCallback, null, null);
	}

	private boolean processFail(Throwable t, boolean isCallback, Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {

		// damn, we failed. This means only that we keep our books and notify our parent JobExecutionVertex
		// the actual computation on the task manager is cleaned up by the TaskManager that noticed the failure

		// we may need to loop multiple times (in the presence of concurrent calls) in order to
		// atomically switch to failed
		while (true) {
			ExecutionState current = this.state;

			if (current == FAILED) {
				// already failed. It is enough to remember once that we failed (its sad enough)
				return false;
			}

			if (current == CANCELED || current == FINISHED) {
				// we are already aborting or are already aborted or we are already finished
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring transition of vertex {} to {} while being {}.", getVertexWithAttempt(), FAILED, current);
				}
				return false;
			}

			if (current == CANCELING) {
				cancelingComplete(userAccumulators, metrics);
				return false;
			}

			if (transitionState(current, FAILED, t)) {
				// success (in a manner of speaking)
				this.failureCause = t;

				updateAccumulatorsAndMetrics(userAccumulators, metrics);

				try {
					final SimpleSlot slot = assignedResource;
					if (slot != null) {
						slot.releaseSlot();
					}
					vertex.getExecutionGraph().deregisterExecution(this);
				}
				finally {
					vertex.executionFailed(this, t);
					terminationFuture.complete(FAILED);
				}

				if (!isCallback && (current == RUNNING || current == DEPLOYING)) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Sending out cancel request, to remove task execution from TaskManager.");
					}

					try {
						if (assignedResource != null) {
							sendCancelRpcCall();
						}
					} catch (Throwable tt) {
						// no reason this should ever happen, but log it to be safe
						LOG.error("Error triggering cancel call while marking task {} as failed.", getVertex().getTaskNameWithSubtaskIndex(), tt);
					}
				}

				// leave the loop
				return true;
			}
		}
	}

	boolean switchToRunning() {

		if (transitionState(DEPLOYING, RUNNING)) {
			sendPartitionInfos();
			return true;
		}
		else {
			// something happened while the call was in progress.
			// it can mean:
			//  - canceling, while deployment was in progress. state is now canceling, or canceled, if the response overtook
			//  - finishing (execution and finished call overtook the deployment answer, which is possible and happens for fast tasks)
			//  - failed (execution, failure, and failure message overtook the deployment answer)

			ExecutionState currentState = this.state;

			if (currentState == FINISHED || currentState == CANCELED) {
				// do nothing, the task was really fast (nice)
				// or it was canceled really fast
			}
			else if (currentState == CANCELING || currentState == FAILED) {
				if (LOG.isDebugEnabled()) {
					// this log statement is guarded because the 'getVertexWithAttempt()' method
					// performs string concatenations 
					LOG.debug("Concurrent canceling/failing of {} while deployment was in progress.", getVertexWithAttempt());
				}
				sendCancelRpcCall();
			}
			else {
				String message = String.format("Concurrent unexpected state transition of task %s to %s while deployment was in progress.",
						getVertexWithAttempt(), currentState);

				if (LOG.isDebugEnabled()) {
					LOG.debug(message);
				}

				// undo the deployment
				sendCancelRpcCall();

				// record the failure
				markFailed(new Exception(message));
			}

			return false;
		}
	}

	/**
	 * This method sends a CancelTask message to the instance of the assigned slot.
	 *
	 * The sending is tried up to NUM_CANCEL_CALL_TRIES times.
	 */
	private void sendCancelRpcCall() {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			CompletableFuture<Acknowledge> cancelResultFuture = FutureUtils.retry(
				() -> taskManagerGateway.cancelTask(attemptId, timeout),
				NUM_CANCEL_CALL_TRIES,
				executor);

			cancelResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					if (failure != null) {
						fail(new Exception("Task could not be canceled.", failure));
					}
				},
				executor);
		}
	}

	private void sendFailIntermediateResultPartitionsRpcCall() {
		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			// TODO For some tests this could be a problem when querying too early if all resources were released
			taskManagerGateway.failPartition(attemptId);
		}
	}

	/**
	 * Update the partition infos on the assigned resource.
	 *
	 * @param partitionInfos for the remote task
	 */
	private void sendUpdatePartitionInfoRpcCall(
			final Iterable<PartitionInfo> partitionInfos) {

		final SimpleSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
			final TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();

			CompletableFuture<Acknowledge> updatePartitionsResultFuture = taskManagerGateway.updatePartitions(attemptId, partitionInfos, timeout);

			updatePartitionsResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					// fail if there was a failure
					if (failure != null) {
						fail(new IllegalStateException("Update task on TaskManager " + taskManagerLocation +
							" failed due to:", failure));
					}
				}, executor);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	/**
	 * Calculates the preferred locations based on the location preference constraint.
	 *
	 * @param locationPreferenceConstraint constraint for the location preference
	 * @return Future containing the collection of preferred locations. This might not be completed if not all inputs
	 * 		have been a resource assigned.
	 */
	@VisibleForTesting
	public CompletableFuture<Collection<TaskManagerLocation>> calculatePreferredLocations(LocationPreferenceConstraint locationPreferenceConstraint) {
		final Collection<CompletableFuture<TaskManagerLocation>> preferredLocationFutures = getVertex().getPreferredLocationsBasedOnInputs();
		final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture;

		switch(locationPreferenceConstraint) {
			case ALL:
				preferredLocationsFuture = FutureUtils.combineAll(preferredLocationFutures);
				break;
			case ANY:
				final ArrayList<TaskManagerLocation> completedTaskManagerLocations = new ArrayList<>(preferredLocationFutures.size());

				for (CompletableFuture<TaskManagerLocation> preferredLocationFuture : preferredLocationFutures) {
					if (preferredLocationFuture.isDone() && !preferredLocationFuture.isCompletedExceptionally()) {
						final TaskManagerLocation taskManagerLocation = preferredLocationFuture.getNow(null);

						if (taskManagerLocation == null) {
							throw new FlinkRuntimeException("TaskManagerLocationFuture was completed with null. This indicates a programming bug.");
						}

						completedTaskManagerLocations.add(taskManagerLocation);
					}
				}

				preferredLocationsFuture = CompletableFuture.completedFuture(completedTaskManagerLocations);
				break;
			default:
				throw new RuntimeException("Unknown LocationPreferenceConstraint " + locationPreferenceConstraint + '.');
		}

		return preferredLocationsFuture;
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
		return transitionState(currentState, targetState, null);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState, Throwable error) {
		// sanity check
		if (currentState.isTerminal()) {
			throw new IllegalStateException("Cannot leave terminal state " + currentState + " to transition to " + targetState + '.');
		}

		if (STATE_UPDATER.compareAndSet(this, currentState, targetState)) {
			markTimestamp(targetState);

			if (error == null) {
				LOG.info("{} ({}) switched from {} to {}.", getVertex().getTaskNameWithSubtaskIndex(), getAttemptId(), currentState, targetState);
			} else {
				LOG.info("{} ({}) switched from {} to {}.", getVertex().getTaskNameWithSubtaskIndex(), getAttemptId(), currentState, targetState, error);
			}

			// make sure that the state transition completes normally.
			// potential errors (in listeners may not affect the main logic)
			try {
				vertex.notifyStateTransition(this, targetState, error);
			}
			catch (Throwable t) {
				LOG.error("Error while notifying execution graph of execution state transition.", t);
			}
			return true;
		} else {
			return false;
		}
	}

	private void markTimestamp(ExecutionState state) {
		markTimestamp(state, System.currentTimeMillis());
	}

	private void markTimestamp(ExecutionState state, long timestamp) {
		this.stateTimestamps[state.ordinal()] = timestamp;
	}

	public String getVertexWithAttempt() {
		return vertex.getTaskNameWithSubtaskIndex() + " - execution #" + attemptNumber;
	}

	// ------------------------------------------------------------------------
	//  Accumulators
	// ------------------------------------------------------------------------
	
	/**
	 * Update accumulators (discarded when the Execution has already been terminated).
	 * @param userAccumulators the user accumulators
	 */
	public void setAccumulators(Map<String, Accumulator<?, ?>> userAccumulators) {
		synchronized (accumulatorLock) {
			if (!state.isTerminal()) {
				this.userAccumulators = userAccumulators;
			}
		}
	}
	
	public Map<String, Accumulator<?, ?>> getUserAccumulators() {
		return userAccumulators;
	}

	@Override
	public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
	}

	@Override
	public int getParallelSubtaskIndex() {
		return getVertex().getParallelSubtaskIndex();
	}
		
	@Override
	public IOMetrics getIOMetrics() {
		return ioMetrics;
	}

	private void updateAccumulatorsAndMetrics(Map<String, Accumulator<?, ?>> userAccumulators, IOMetrics metrics) {
		if (userAccumulators != null) {
			synchronized (accumulatorLock) {
				this.userAccumulators = userAccumulators;
			}
		}
		if (metrics != null) {
			this.ioMetrics = metrics;
		}
	}

	// ------------------------------------------------------------------------
	//  Standard utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		final SimpleSlot slot = assignedResource;

		return String.format("Attempt #%d (%s) @ %s - [%s]", attemptNumber, vertex.getTaskNameWithSubtaskIndex(),
				(slot == null ? "(unassigned)" : slot), state);
	}

	@Override
	public ArchivedExecution archive() {
		return new ArchivedExecution(this);
	}
}
