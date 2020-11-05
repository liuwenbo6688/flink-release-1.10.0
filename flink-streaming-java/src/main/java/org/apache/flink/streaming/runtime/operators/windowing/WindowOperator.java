/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link InternalWindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 *
 * WindowOperator负责window中元素存储和计算流程
 *
 * WindowOperator包含如下几个重要方法：
 *
 * 		open():  operator初始化的逻辑
 * 	    processElement(): 新元素进入window的时候调用
 * 	    onEventTime(): event time计算触发时候的逻辑
 * 	    onProcessingTime(): processing time计算触发时候的逻辑
 */
@Internal
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<? super IN, W> windowAssigner;

	private final KeySelector<IN, K> keySelector;

	private final Trigger<? super IN, ? super W> trigger;

	private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	/** For serializing the key in checkpoints. */
	protected final TypeSerializer<K> keySerializer;

	/** For serializing the window in checkpoints. */
	protected final TypeSerializer<W> windowSerializer;

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 *     <li>Deciding if an element should be dropped from a window due to lateness.
	 *     <li>Clearing the state of a window if the system time passes the
	 *         {@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	protected final long allowedLateness;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements for which
	 * {@code window.maxTimestamp + allowedLateness} is smaller than the current watermark will
	 * be emitted to this.
	 */
	protected final OutputTag<IN> lateDataOutputTag;

	private static final  String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

	protected transient Counter numLateRecordsDropped;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/** The state in which the window contents is stored. Each window is a namespace */
	// 用于储存窗口中的数据
	private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;

	/**
	 * The {@link #windowState}, typed to merging state for merging windows.
	 * Null if the window state is not mergeable.
	 */
	private transient InternalMergingState<K, W, IN, ACC, ACC> windowMergingState;

	/** The state that holds the merging window metadata (the sets that describe what is merged). */
	private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given
	 * timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	protected transient Context triggerContext = new Context(null, null);

	protected transient WindowContext processContext;

	protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	protected transient InternalTimerService<W> internalTimerService;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(
			WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
			InternalWindowFunction<ACC, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			long allowedLateness,
			OutputTag<IN> lateDataOutputTag) {

		super(windowFunction);

		checkArgument(!(windowAssigner instanceof BaseAlignedWindowAssigner),
			"The " + windowAssigner.getClass().getSimpleName() + " cannot be used with a WindowOperator. " +
				"This assigner is only used with the AccumulatingProcessingTimeWindowOperator and " +
				"the AggregatingProcessingTimeWindowOperator");

		checkArgument(allowedLateness >= 0);

		checkArgument(windowStateDescriptor == null || windowStateDescriptor.isSerializerInitialized(),
				"window state serializer is not properly initialized");

		/**
		 *
		 */
		this.windowAssigner = checkNotNull(windowAssigner);


		this.windowSerializer = checkNotNull(windowSerializer);

		/**
		 *
		 */
		this.keySelector = checkNotNull(keySelector);

		this.keySerializer = checkNotNull(keySerializer);
		this.windowStateDescriptor = windowStateDescriptor;

		/**
		 *
		 */
		this.trigger = checkNotNull(trigger);

		/**
		 *
		 */
		this.allowedLateness = allowedLateness;

		this.lateDataOutputTag = lateDataOutputTag;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	/**
	 * operator 初始化的逻辑
	 * open()方法在operator处理任何元素之前调用
	 * open()方法负责operator的初始化工作，例如状态初始化
	 * 如果该方法抛出异常，operator会失败，无法进行后续处理数据的流程
	 *
	 * 在 StreamTask#initializeStateAndOpen() 方法中被调用
	 *
	 */
	@Override
	public void open() throws Exception {
		super.open();

		// 打开迟到被丢弃数据条数统计的监控
		this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);

		// 创建一个基于时间戳的数据收集器，用于输出窗口数据到下游
		timestampedCollector = new TimestampedCollector<>(output);

		// 获取时间服务，用于向windowAssignerContext传递当前的processing time
		internalTimerService =
				getInternalTimerService("window-timers", windowSerializer, this);

		triggerContext = new Context(null, null);
		processContext = new WindowContext(null);

		/**
		 *  创建 WindowAssignerContext，主要用于获取processing time
		 */
		windowAssignerContext = new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				return internalTimerService.currentProcessingTime();
			}
		};


		// ********** 创建windowState，用于储存窗口中的数据 ***********
		if (windowStateDescriptor != null) {
			windowState = (InternalAppendingState<K, W, IN, ACC, ACC>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
		}

		// create the typed and helper states for merging windows
		if (windowAssigner instanceof MergingWindowAssigner) {
			// 如果 windowAssigner 是MergingWindowAssigner子类，即使用的是SessionWindow的话

			// store a typed reference for the state of merging windows - sanity check
			if (windowState instanceof InternalMergingState) {
				windowMergingState = (InternalMergingState<K, W, IN, ACC, ACC>) windowState;
			}

			/**
			 *  以下逻辑为创建储存window合并的状态变量 mergingSetsState
			 */
			@SuppressWarnings("unchecked")
			final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

			final TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>(
					typedTuple,
					new TypeSerializer[] {windowSerializer, windowSerializer});

			final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
					new ListStateDescriptor<>("merging-window-set", tupleSerializer);

			// get the state that stores the merging sets
			mergingSetsState = (InternalListState<K, VoidNamespace, Tuple2<W, W>>)
					getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mergingSetsStateDescriptor);
			mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		timestampedCollector = null;
		triggerContext = null;
		processContext = null;
		windowAssignerContext = null;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		timestampedCollector = null;
		triggerContext = null;
		processContext = null;
		windowAssignerContext = null;
	}

	/**
	 *  新元素进入window的时候调用
	 *  有数据到达 window 的时候，系统会调用processElement方法
	 */
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		/**
		 * 获取需要分配给该元素的 window
		 * 以EventTime时间窗口为例，element的 EventTime 位于window的起止时间之中，则该窗口需要指派给此元素
		 */
		final Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(),
																			element.getTimestamp(),
																			windowAssignerContext );

		//if element is handled by none of assigned elementWindows
		// 返回元素是否被处理，如果元素经过处理，返回false
		boolean isSkippedElement = true;


		/**
		 *  此处获取到key的值，即keyBy方法字段的值
		 *  该KeyedStateBackend在 StreamTaskStateInitializerImpl 中创建
		 */
		final K key = this.<K>getKeyedStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			// 判断Window 是否是MergingWindowAssigner(合并窗口)的子类。比如 SessionWindow 属于MergingWindowAssigner
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {

						if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
							throw new UnsupportedOperationException("The end timestamp of an " +
									"event-time window cannot become earlier than the current watermark " +
									"by merging. Current watermark: " + internalTimerService.currentWatermark() +
									" window: " + mergeResult);
						} else if (!windowAssigner.isEventTime()) {
							long currentProcessingTime = internalTimerService.currentProcessingTime();
							if (mergeResult.maxTimestamp() <= currentProcessingTime) {
								throw new UnsupportedOperationException("The end timestamp of a " +
									"processing-time window cannot become earlier than the current processing time " +
									"by merging. Current processing time: " + currentProcessingTime +
									" window: " + mergeResult);
							}
						}

						triggerContext.key = key;
						triggerContext.window = mergeResult;

						triggerContext.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							triggerContext.window = m;
							triggerContext.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
					}
				});

				// drop if the window is already late
				if (isWindowLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}
				isSkippedElement = false;

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				windowState.setCurrentNamespace(stateWindow);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = actualWindow;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(actualWindow, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			// 非 MergingWindowAssigner 部分的处理逻辑
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isWindowLate(window)) {
					// 判断window是否迟到，如果已经迟到的窗口就丢弃
					continue;
				}
				isSkippedElement = false; // 标记元素是否被跳过

				/**
				 * windowState 为 HeapListState
				 * HeapListState为内存中存储的分区化的链表状态(ListState)
				 * 使用namespace区分不同窗口的数据
				 * 可以理解为一个Map: key为window对象，value为元素的值
				 */
				windowState.setCurrentNamespace(window);
				windowState.add(element.getValue());

				// 设置 trigger 上下文对象的key和window
				triggerContext.key = key;
				triggerContext.window = window;

				// 调用trigger的onElement方法，询问trigger新element到来的时候需要作出什么动作。
				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) { // isFire表示需要触发计算
					// 取出windowState当前namespace下所有的元素。即当前window下所有的元素
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					// 使用用户传入的处理函数来计算window内数据
					emitWindowContents(window, contents);
				}

				if (triggerResult.isPurge()) {
					// 如果触发器返回需要清空数据，则删除window中所有的数据
					windowState.clear();
				}

				/**
				 *  注册 timer
				 *  当前时间已经过了window的cleanup时间（里面有cleanup time的含义，就是 window.maxTimestamp + allowedLateness）
				 *  会根据窗口的类型调用对应的onProcessingTime方法或者是onEventTime方法
				 */
				registerCleanupTimer(window);
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if (isSkippedElement && isElementLate(element)) {
			/**
			 *  如果元素没有被window处理，并且元素来迟，会加入到旁路输出
			 *  否则此数据被丢弃，迟到被丢弃数据条数监控会增加1
			 */
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}

	/**
	 *  event time 计算触发时候的逻辑
	 */
	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		// 获取key和window
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		// 调用trigger的onEventTime方法
		TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

		// 此处为触发计算的逻辑
		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		// 如果触发了purge操作，则清空window中的内容
		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		// 如果是event time类型，并且定时器触发时间是window的cleanup时间的时候，意味着该窗口的数据已经处理完毕，需要清除该窗口的所有状态
		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	/**
	 *  processing time 计算触发时候的逻辑
	 */
	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		/**
		 * onProcessingTime方法和onEventTime方法相比，
		 * 除了调用trigger的onProcessingTime方法这一处不同外，其他的的逻辑基本类似
		 */
		TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	/**
	 * Drops all state for the given window and calls
	 * {@link Trigger#clear(Window, Trigger.TriggerContext)}.
	 *
	 * <p>The caller must ensure that the
	 * correct key is set in the state backend and the triggerContext object.
	 */
	private void clearAllState(
			W window,
			AppendingState<IN, ACC> windowState,
			MergingWindowSet<W> mergingWindows) throws Exception {
		windowState.clear();
		triggerContext.clear();
		processContext.window = window;
		processContext.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
			mergingWindows.persist();
		}
	}

	/**
	 * Emits the contents of the given window using the {@link InternalWindowFunction}.
	 */
	@SuppressWarnings("unchecked")
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		processContext.window = window;
		/**
		 * 此处调用用户编写的处理函数
		 * 此处用户的函数，比如 ProcessWindowFunction 被 InternalIterableProcessWindowFunction 包装了
		 */
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}

	/**
	 * Write skipped late arriving element to SideOutput.
	 *
	 * @param element skipped late arriving element to side output
	 */
	protected void sideOutput(StreamRecord<IN> element){
		output.collect(lateDataOutputTag, element);
	}

	/**
	 * Retrieves the {@link MergingWindowSet} for the currently active key.
	 * The caller must ensure that the correct key is set in the state backend.
	 *
	 * <p>The caller must also ensure to properly persist changes to state using
	 * {@link MergingWindowSet#persist()}.
	 */
	protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
		@SuppressWarnings("unchecked")
		MergingWindowAssigner<? super IN, W> mergingAssigner = (MergingWindowAssigner<? super IN, W>) windowAssigner;
		return new MergingWindowSet<>(mergingAssigner, mergingSetsState);
	}

	/**
	 * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness
	 * of the given window.
	 */
	protected boolean isWindowLate(W window) {
		/**
		 * 如果window类型为 EventTime 并且window的cleanup时间比当前 watermark 早，说明window已经迟到
		 */
		return (windowAssigner.isEventTime() && (cleanupTime(window) <= internalTimerService.currentWatermark()));
	}

	/**
	 * Decide if a record is currently late, based on current watermark and allowed lateness.
	 *
	 * @param element The element to check
	 * @return The element for which should be considered when sideoutputs
	 */
	protected boolean isElementLate(StreamRecord<IN> element){
		return (windowAssigner.isEventTime()) &&
			(element.getTimestamp() + allowedLateness <= internalTimerService.currentWatermark());
	}

	/**
	 * Registers a timer to cleanup the content of the window.
	 * @param window
	 * 					the window whose state to discard
	 */
	protected void registerCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// don't set a GC timer for "end of time"
			return;
		}
		// 增加对应 onProcessingTime 或 onEventTime 定时器
		if (windowAssigner.isEventTime()) {
			triggerContext.registerEventTimeTimer(cleanupTime);
		} else {
			triggerContext.registerProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Deletes the cleanup timer set for the contents of the provided window.
	 * @param window
	 * 					the window whose state to discard
	 */
	protected void deleteCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// no need to clean up because we didn't set one
			return;
		}
		if (windowAssigner.isEventTime()) {
			triggerContext.deleteEventTimeTimer(cleanupTime);
		} else {
			triggerContext.deleteProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Returns the cleanup time for a window, which is
	 * {@code window.maxTimestamp + allowedLateness}. In
	 * case this leads to a value greater than {@link Long#MAX_VALUE}
	 * then a cleanup time of {@link Long#MAX_VALUE} is
	 * returned.
	 *
	 * @param window the window whose cleanup time we are computing.
	 */
	private long cleanupTime(W window) {
		if (windowAssigner.isEventTime()) {

			// cleanup time为window的end时间 + 允许迟到的时间 - 1
			long cleanupTime = window.maxTimestamp() + allowedLateness;

			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return window.maxTimestamp();
		}
	}

	/**
	 * Returns {@code true} if the given time is the cleanup time for the given window.
	 */
	protected final boolean isCleanupTime(W window, long time) {
		return time == cleanupTime(window);
	}

	/**
	 * Base class for per-window {@link KeyedStateStore KeyedStateStores}. Used to allow per-window
	 * state access for {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
	 */
	public abstract class AbstractPerWindowStateStore extends DefaultKeyedStateStore {

		// we have this in the base class even though it's not used in MergingKeyStore so that
		// we can always set it and ignore what actual implementation we have
		protected W window;

		public AbstractPerWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
			super(keyedStateBackend, executionConfig);
		}
	}

	/**
	 * Special {@link AbstractPerWindowStateStore} that doesn't allow access to per-window state.
	 */
	public class MergingWindowStateStore extends AbstractPerWindowStateStore {

		public MergingWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
			super(keyedStateBackend, executionConfig);
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <T, A> FoldingState<T, A> getFoldingState(FoldingStateDescriptor<T, A> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}
	}

	/**
	 * Regular per-window state store for use with
	 * {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
	 */
	public class PerWindowStateStore extends AbstractPerWindowStateStore {

		public PerWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
			super(keyedStateBackend, executionConfig);
		}

		@Override
		protected  <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
			return keyedStateBackend.getPartitionedState(
				window,
				windowSerializer,
				stateDescriptor);
		}
	}

	/**
	 * A utility class for handling {@code ProcessWindowFunction} invocations. This can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in the
	 * {@code WindowContext}.
	 */
	public class WindowContext implements InternalWindowFunction.InternalWindowContext {
		protected W window;

		protected AbstractPerWindowStateStore windowState;

		public WindowContext(W window) {
			this.window = window;
			this.windowState = windowAssigner instanceof MergingWindowAssigner ?
				new MergingWindowStateStore(getKeyedStateBackend(), getExecutionConfig()) :
				new PerWindowStateStore(getKeyedStateBackend(), getExecutionConfig());
		}

		@Override
		public String toString() {
			return "WindowContext{Window = " + window.toString() + "}";
		}

		public void clear() throws Exception {
			userFunction.clear(window, this);
		}

		@Override
		public long currentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public KeyedStateStore windowState() {
			this.windowState.window = this.window;
			return this.windowState;
		}

		@Override
		public KeyedStateStore globalState() {
			return WindowOperator.this.getKeyedStateStore();
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}
			output.collect(outputTag, new StreamRecord<>(value, window.maxTimestamp()));
		}
	}

	/**
	 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code Context}
	 */
	public class Context implements Trigger.OnMergeContext {
		protected K key;
		protected W window;

		protected Collection<W> mergedWindows;

		public Context(K key, W window) {
			this.key = key;
			this.window = window;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return WindowOperator.this.getMetricGroup();
		}

		public long getCurrentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
			checkNotNull(stateType, "The state type class must not be null");

			TypeInformation<S> typeInfo;
			try {
				typeInfo = TypeExtractor.getForClass(stateType);
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
					"' from the class alone, due to generic type parameters. " +
					"Please specify the TypeInformation directly.", e);
			}

			return getKeyValueState(name, typeInfo, defaultState);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			TypeInformation<S> stateType,
			S defaultState) {

			checkNotNull(name, "The name of the state must not be null");
			checkNotNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return WindowOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					S rawState = getKeyedStateBackend().getOrCreateKeyedState(windowSerializer, stateDescriptor);

					if (rawState instanceof InternalMergingState) {
						@SuppressWarnings("unchecked")
						InternalMergingState<K, W, ?, ?, ?> mergingState = (InternalMergingState<K, W, ?, ?, ?>) rawState;
						mergingState.mergeNamespaces(window, mergedWindows);
					}
					else {
						throw new IllegalArgumentException(
								"The given state descriptor does not refer to a mergeable state (MergingState)");
					}
				}
				catch (Exception e) {
					throw new RuntimeException("Error while merging state.", e);
				}
			}
		}

		@Override
		public long getCurrentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			internalTimerService.registerProcessingTimeTimer(window, time);
		}

		@Override
		public void registerEventTimeTimer(long time) {
			internalTimerService.registerEventTimeTimer(window, time);
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			internalTimerService.deleteProcessingTimeTimer(window, time);
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			internalTimerService.deleteEventTimeTimer(window, time);
		}

		public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window, this);
		}

		public TriggerResult onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window, this);
		}

		public void onMerge(Collection<W> mergedWindows) throws Exception {
			this.mergedWindows = mergedWindows;
			trigger.onMerge(window, this);
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}

		@Override
		public String toString() {
			return "Context{" +
				"key=" + key +
				", window=" + window +
				'}';
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
		protected long timestamp;
		protected K key;
		protected W window;

		public Timer(long timestamp, K key, W window) {
			this.timestamp = timestamp;
			this.key = key;
			this.window = window;
		}

		@Override
		public int compareTo(Timer<K, W> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& window.equals(timer.window);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + window.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", window=" + window +
				'}';
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
	}

	@VisibleForTesting
	public KeySelector<IN, K> getKeySelector() {
		return keySelector;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return windowStateDescriptor;
	}
}
