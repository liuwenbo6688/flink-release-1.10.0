/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;

import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.contrib.streaming.state.RocksDBConfigurableOptions.WRITE_BATCH_SIZE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.TIMER_SERVICE_FACTORY;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.TTL_COMPACT_FILTER_ENABLED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A State Backend that stores its state in {@code RocksDB}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of RocksDB.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * RocksDB database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 *
 * <p>The behavior of the RocksDB instances can be parametrized by setting RocksDB Options
 * using the methods {@link #setPredefinedOptions(PredefinedOptions)} and
 * {@link #setRocksDBOptions(RocksDBOptionsFactory)}.
 */
public class RocksDBStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

	/**
	 * The options to chose for the type of priority queue state.
	 */
	public enum PriorityQueueStateType {
		HEAP,
		ROCKSDB
	}

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateBackend.class);

	/** The number of (re)tries for loading the RocksDB JNI library. */
	private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

	/** Flag whether the native library has been loaded. */
	private static boolean rocksDbInitialized = false;

	private static final int UNDEFINED_NUMBER_OF_TRANSFER_THREADS = -1;
	private static final long UNDEFINED_WRITE_BATCH_SIZE = -1;

	// ------------------------------------------------------------------------

	// -- configuration values, set in the application / configuration

	/** The state backend that we use for creating checkpoint streams. */
	private final StateBackend checkpointStreamBackend;

	/** Base paths for RocksDB directory, as configured.
	 * Null if not yet set, in which case the configuration values will be used.
	 * The configuration defaults to the TaskManager's temp directories. */
	@Nullable
	private File[] localRocksDbDirectories;

	/** The pre-configured option settings. */
	@Nullable
	private PredefinedOptions predefinedOptions;

	/** The options factory to create the RocksDB options in the cluster. */
	@Nullable
	private RocksDBOptionsFactory rocksDbOptionsFactory;

	/** This determines if incremental checkpointing is enabled. */
	private final TernaryBoolean enableIncrementalCheckpointing;

	/** Thread number used to transfer (download and upload) state, default value: 1. */
	private int numberOfTransferThreads;

	/**
	 * This determines if compaction filter to cleanup state with TTL is enabled.
	 *
	 * <p>Note: User can still decide in state TTL configuration in state descriptor
	 * whether the filter is active for particular state or not.
	 */
	private TernaryBoolean enableTtlCompactionFilter;

	/** The configuration for memory settings (pool sizes, etc.). */
	private final RocksDBMemoryConfiguration memoryConfiguration;

	/** This determines the type of priority queue state. */
	private final PriorityQueueStateType priorityQueueStateType;

	/** The default rocksdb metrics options. */
	private final RocksDBNativeMetricOptions defaultMetricOptions;

	// -- runtime values, set on TaskManager when initializing / using the backend

	/** Base paths for RocksDB directory, as initialized.
	 *  这里就是我们上述设置的 多个 rocksdb local dir
	 * */
	private transient File[] initializedDbBasePaths;

	/** JobID for uniquifying backup paths. */
	private transient JobID jobId;

	/** The index of the next directory to be used from {@link #initializedDbBasePaths}.*/
	/**
	 * 下一次要使用 dir 的 index，如果 nextDirectory = 2，
	 * 则使用 initializedDbBasePaths 中下标为 2 的那个目录做为 rocksdb 的存储目录
	 */
	private transient int nextDirectory;

	/** Whether we already lazily initialized our local storage directories. */
	private transient boolean isInitialized;

	/**
	 * Max consumed memory size for one batch in {@link RocksDBWriteBatchWrapper}, default value 2mb.
	 */
	private long writeBatchSize;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 *
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public RocksDBStateBackend(String checkpointDataUri) throws IOException {
		this(new Path(checkpointDataUri).toUri());
	}

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 *
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public RocksDBStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing) throws IOException {
		this(new Path(checkpointDataUri).toUri(), enableIncrementalCheckpointing);
	}

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 *
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public RocksDBStateBackend(URI checkpointDataUri) throws IOException {
		this(new FsStateBackend(checkpointDataUri));
	}

	/**
	 * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
	 * file system and location defined by the given URI.
	 *
	 * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
	 * host and port in the URI, or have the Hadoop configuration that describes the file system
	 * (host / high-availability group / possibly credentials) either referenced from the Flink
	 * config, or included in the classpath.
	 *
	 * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
	 * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
	 * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
	 */
	public RocksDBStateBackend(URI checkpointDataUri, boolean enableIncrementalCheckpointing) throws IOException {
		this(new FsStateBackend(checkpointDataUri), enableIncrementalCheckpointing);
	}

	/**
	 * Creates a new {@code RocksDBStateBackend} that uses the given state backend to store its
	 * checkpoint data streams. Typically, one would supply a filesystem or database state backend
	 * here where the snapshots from RocksDB would be stored.
	 *
	 * <p>The snapshots of the RocksDB state will be stored using the given backend's
	 * {@link StateBackend#createCheckpointStorage(JobID)}.
	 *
	 * @param checkpointStreamBackend The backend write the checkpoint streams to.
	 */
	public RocksDBStateBackend(StateBackend checkpointStreamBackend) {
		this(checkpointStreamBackend, TernaryBoolean.UNDEFINED);
	}

	/**
	 * Creates a new {@code RocksDBStateBackend} that uses the given state backend to store its
	 * checkpoint data streams. Typically, one would supply a filesystem or database state backend
	 * here where the snapshots from RocksDB would be stored.
	 *
	 * <p>The snapshots of the RocksDB state will be stored using the given backend's
	 * {@link StateBackend#createCheckpointStorage(JobID)}.
	 *
	 * @param checkpointStreamBackend The backend write the checkpoint streams to.
	 * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
	 */
	public RocksDBStateBackend(StateBackend checkpointStreamBackend, TernaryBoolean enableIncrementalCheckpointing) {
		this.checkpointStreamBackend = checkNotNull(checkpointStreamBackend);
		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.numberOfTransferThreads = UNDEFINED_NUMBER_OF_TRANSFER_THREADS;
		// use RocksDB-based implementation as default from FLINK-15637
		this.priorityQueueStateType = PriorityQueueStateType.ROCKSDB;
		this.defaultMetricOptions = new RocksDBNativeMetricOptions();
		this.enableTtlCompactionFilter = TernaryBoolean.UNDEFINED;
		this.memoryConfiguration = new RocksDBMemoryConfiguration();
		this.writeBatchSize = UNDEFINED_WRITE_BATCH_SIZE;
	}

	/**
	 * @deprecated Use {@link #RocksDBStateBackend(StateBackend)} instead.
	 */
	@Deprecated
	public RocksDBStateBackend(AbstractStateBackend checkpointStreamBackend) {
		this(checkpointStreamBackend, TernaryBoolean.UNDEFINED);
	}

	/**
	 * @deprecated Use {@link #RocksDBStateBackend(StateBackend, TernaryBoolean)} instead.
	 */
	@Deprecated
	public RocksDBStateBackend(AbstractStateBackend checkpointStreamBackend, boolean enableIncrementalCheckpointing) {
		this(checkpointStreamBackend, TernaryBoolean.fromBoolean(enableIncrementalCheckpointing));
	}

	/**
	 * Private constructor that creates a re-configured copy of the state backend.
	 *
	 * @param original The state backend to re-configure.
	 * @param config The configuration.
	 * @param classLoader The class loader.
	 */
	private RocksDBStateBackend(RocksDBStateBackend original, Configuration config, ClassLoader classLoader) {
		// reconfigure the state backend backing the streams
		final StateBackend originalStreamBackend = original.checkpointStreamBackend;
		this.checkpointStreamBackend = originalStreamBackend instanceof ConfigurableStateBackend ?
				((ConfigurableStateBackend) originalStreamBackend).configure(config, classLoader) :
				originalStreamBackend;

		// configure incremental checkpoints
		this.enableIncrementalCheckpointing = original.enableIncrementalCheckpointing.resolveUndefined(
			config.getBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS));

		if (original.numberOfTransferThreads == UNDEFINED_NUMBER_OF_TRANSFER_THREADS) {
			this.numberOfTransferThreads = config.getInteger(CHECKPOINT_TRANSFER_THREAD_NUM);
		} else {
			this.numberOfTransferThreads = original.numberOfTransferThreads;
		}

		if (original.writeBatchSize == UNDEFINED_WRITE_BATCH_SIZE) {
			this.writeBatchSize = config.get(WRITE_BATCH_SIZE).getBytes();
		} else {
			this.writeBatchSize = original.writeBatchSize;
		}
		this.enableTtlCompactionFilter = original.enableTtlCompactionFilter
			.resolveUndefined(config.getBoolean(TTL_COMPACT_FILTER_ENABLED));

		this.memoryConfiguration = RocksDBMemoryConfiguration.fromOtherAndConfiguration(original.memoryConfiguration, config);
		this.memoryConfiguration.validate();

		final String priorityQueueTypeString = config.getString(TIMER_SERVICE_FACTORY);

		this.priorityQueueStateType = priorityQueueTypeString.length() > 0 ?
			PriorityQueueStateType.valueOf(priorityQueueTypeString.toUpperCase()) : original.priorityQueueStateType;

		// configure local directories
		if (original.localRocksDbDirectories != null) {
			this.localRocksDbDirectories = original.localRocksDbDirectories;
		}
		else {
			final String rocksdbLocalPaths = config.getString(RocksDBOptions.LOCAL_DIRECTORIES);
			if (rocksdbLocalPaths != null) {
				String[] directories = rocksdbLocalPaths.split(",|" + File.pathSeparator);

				try {
					setDbStoragePaths(directories);
				}
				catch (IllegalArgumentException e) {
					throw new IllegalConfigurationException("Invalid configuration for RocksDB state " +
							"backend's local storage directories: " + e.getMessage(), e);
				}
			}
		}

		// configure metric options
		this.defaultMetricOptions = RocksDBNativeMetricOptions.fromConfig(config);

		// configure RocksDB predefined options
		this.predefinedOptions = original.predefinedOptions == null ?
			PredefinedOptions.valueOf(config.getString(RocksDBOptions.PREDEFINED_OPTIONS)) : original.predefinedOptions;
		LOG.info("Using predefined options: {}.", predefinedOptions.name());

		// configure RocksDB options factory
		try {
			rocksDbOptionsFactory = configureOptionsFactory(
				original.rocksDbOptionsFactory,
				config.getString(RocksDBOptions.OPTIONS_FACTORY),
				config,
				classLoader);
		} catch (DynamicCodeLoadingException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	// ------------------------------------------------------------------------
	//  Reconfiguration
	// ------------------------------------------------------------------------

	/**
	 * Creates a copy of this state backend that uses the values defined in the configuration
	 * for fields where that were not yet specified in this state backend.
	 *
	 * @param config The configuration.
	 * @param classLoader The class loader.
	 * @return The re-configured variant of the state backend
	 */
	@Override
	public RocksDBStateBackend configure(Configuration config, ClassLoader classLoader) {
		return new RocksDBStateBackend(this, config, classLoader);
	}

	// ------------------------------------------------------------------------
	//  State backend methods
	// ------------------------------------------------------------------------

	/**
	 * Gets the state backend that this RocksDB state backend uses to persist
	 * its bytes to.
	 *
	 * <p>This RocksDB state backend only implements the RocksDB specific parts, it
	 * relies on the 'CheckpointBackend' to persist the checkpoint and savepoint bytes
	 * streams.
	 */
	public StateBackend getCheckpointBackend() {
		return checkpointStreamBackend;
	}

	private void lazyInitializeForJob(
			Environment env,
			@SuppressWarnings("unused") String operatorIdentifier) throws IOException {

		if (isInitialized) {
			return;
		}

		this.jobId = env.getJobID();

		// initialize the paths where the local RocksDB files should be stored
		if (localRocksDbDirectories == null) {
			// initialize from the temp directories
			initializedDbBasePaths = env.getIOManager().getSpillingDirectories();
		}
		else {
			List<File> dirs = new ArrayList<>(localRocksDbDirectories.length);
			StringBuilder errorMessage = new StringBuilder();

			for (File f : localRocksDbDirectories) {
				File testDir = new File(f, UUID.randomUUID().toString());
				if (!testDir.mkdirs()) {
					String msg = "Local DB files directory '" + f
							+ "' does not exist and cannot be created. ";
					LOG.error(msg);
					errorMessage.append(msg);
				} else {
					dirs.add(f);
				}
				//noinspection ResultOfMethodCallIgnored
				testDir.delete();
			}

			if (dirs.isEmpty()) {
				throw new IOException("No local storage directories available. " + errorMessage);
			} else {
				initializedDbBasePaths = dirs.toArray(new File[dirs.size()]);
			}
		}

		/**
		 * lazyInitializeForJob 方法中， 通过这一行代码决定下一次要使用 dir 的 index，
		 * 根据 initializedDbBasePaths.length 生成随机数，
		 * 如果 initializedDbBasePaths.length = 12，生成随机数的范围为 0-11
		 *
		 * 随机分配有小概率会出现冲突
		 */
		nextDirectory = new Random().nextInt(initializedDbBasePaths.length);

		isInitialized = true;
	}

	private File getNextStoragePath() {
		int ni = nextDirectory + 1;
		ni = ni >= initializedDbBasePaths.length ? 0 : ni;
		nextDirectory = ni;

		return initializedDbBasePaths[ni];
	}

	// ------------------------------------------------------------------------
	//  Checkpoint initialization and persistent storage
	// ------------------------------------------------------------------------

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer) throws IOException {
		return checkpointStreamBackend.resolveCheckpoint(pointer);
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		return checkpointStreamBackend.createCheckpointStorage(jobId);
	}

	// ------------------------------------------------------------------------
	//  State holding data structures
	// ------------------------------------------------------------------------

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws IOException {

		// first, make sure that the RocksDB JNI library is loaded
		// we do this explicitly here to have better error handling
		String tempDir = env.getTaskManagerInfo().getTmpDirectories()[0];
		ensureRocksDBIsLoaded(tempDir);

		// replace all characters that are not legal for filenames with underscore
		String fileCompatibleIdentifier = operatorIdentifier.replaceAll("[^a-zA-Z0-9\\-]", "_");

		/**
		 * rockdb 初始化磁盘目录的逻辑
		 */
		lazyInitializeForJob(env, fileCompatibleIdentifier);
		File instanceBasePath = new File(
			getNextStoragePath(), // 选择一块磁盘
			"job_" + jobId + "_op_" + fileCompatibleIdentifier + "_uuid_" + UUID.randomUUID());

		LocalRecoveryConfig localRecoveryConfig =
			env.getTaskStateManager().createLocalRecoveryConfig();

		final OpaqueMemoryResource<RocksDBSharedResources> sharedResources = RocksDBOperationUtils
				.allocateSharedCachesIfConfigured(memoryConfiguration, env.getMemoryManager(), LOG);
		if (sharedResources != null) {
			LOG.info("Obtained shared RocksDB cache of size {} bytes", sharedResources.getSize());
		}
		final RocksDBResourceContainer resourceContainer = createOptionsAndResourceContainer(sharedResources);

		ExecutionConfig executionConfig = env.getExecutionConfig();
		StreamCompressionDecorator keyGroupCompressionDecorator = getCompressionDecorator(executionConfig);
		RocksDBKeyedStateBackendBuilder<K> builder = new RocksDBKeyedStateBackendBuilder<>(
			operatorIdentifier,
			env.getUserClassLoader(),
			instanceBasePath,
			resourceContainer,
			stateName -> resourceContainer.getColumnOptions(),
			kvStateRegistry,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			executionConfig,
			localRecoveryConfig,
			priorityQueueStateType,
			ttlTimeProvider,
			metricGroup,
			stateHandles,
			keyGroupCompressionDecorator,
			cancelStreamRegistry
		)
			.setEnableIncrementalCheckpointing(isIncrementalCheckpointsEnabled())
			.setEnableTtlCompactionFilter(isTtlCompactionFilterEnabled())
			.setNumberOfTransferingThreads(getNumberOfTransferThreads())
			.setNativeMetricOptions(resourceContainer.getMemoryWatcherOptions(defaultMetricOptions))
			.setWriteBatchSize(getWriteBatchSize());
		return builder.build();
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
		Environment env,
		String operatorIdentifier,
		@Nonnull Collection<OperatorStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception {

		//the default for RocksDB; eventually there can be a operator state backend based on RocksDB, too.
		final boolean asyncSnapshots = true;
		return new DefaultOperatorStateBackendBuilder(
			env.getUserClassLoader(),
			env.getExecutionConfig(),
			asyncSnapshots,
			stateHandles,
			cancelStreamRegistry).build();
	}

	private RocksDBOptionsFactory configureOptionsFactory(
			@Nullable RocksDBOptionsFactory originalOptionsFactory,
			String factoryClassName,
			Configuration config,
			ClassLoader classLoader) throws DynamicCodeLoadingException {

		if (originalOptionsFactory != null) {
			if (originalOptionsFactory instanceof ConfigurableRocksDBOptionsFactory) {
				originalOptionsFactory = ((ConfigurableRocksDBOptionsFactory) originalOptionsFactory).configure(config);
			}
			LOG.info("Using application-defined options factory: {}.", originalOptionsFactory);

			return originalOptionsFactory;
		}

		// if using DefaultConfigurableOptionsFactory by default, we could avoid reflection to speed up.
		if (factoryClassName.equalsIgnoreCase(DefaultConfigurableOptionsFactory.class.getName())) {
			DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
			optionsFactory.configure(config);
			LOG.info("Using default options factory: {}.", optionsFactory);

			return optionsFactory;
		} else {
			try {
				@SuppressWarnings("rawtypes")
				Class<? extends RocksDBOptionsFactory> clazz =
					Class.forName(factoryClassName, false, classLoader)
						.asSubclass(RocksDBOptionsFactory.class);

				RocksDBOptionsFactory optionsFactory = clazz.newInstance();
				if (optionsFactory instanceof ConfigurableRocksDBOptionsFactory) {
					optionsFactory = ((ConfigurableRocksDBOptionsFactory) optionsFactory).configure(config);
				}
				LOG.info("Using configured options factory: {}.", optionsFactory);

				return optionsFactory;
			} catch (ClassNotFoundException e) {
				throw new DynamicCodeLoadingException(
					"Cannot find configured options factory class: " + factoryClassName, e);
			} catch (ClassCastException | InstantiationException | IllegalAccessException e) {
				throw new DynamicCodeLoadingException("The class configured under '" +
					RocksDBOptions.OPTIONS_FACTORY.key() + "' is not a valid options factory (" +
					factoryClassName + ')', e);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Parameters
	// ------------------------------------------------------------------------

	/**
	 * Gets the memory configuration object, which offers settings to control RocksDB's memory usage.
	 */
	public RocksDBMemoryConfiguration getMemoryConfiguration() {
		return memoryConfiguration;
	}

	/**
	 * Sets the path where the RocksDB local database files should be stored on the local
	 * file system. Setting this path overrides the default behavior, where the
	 * files are stored across the configured temp directories.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param path The path where the local RocksDB database files are stored.
	 */
	public void setDbStoragePath(String path) {
		setDbStoragePaths(path == null ? null : new String[] { path });
	}

	/**
	 * Sets the directories in which the local RocksDB database puts its files (like SST and
	 * metadata files). These directories do not need to be persistent, they can be ephemeral,
	 * meaning that they are lost on a machine failure, because state in RocksDB is persisted
	 * in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 *
	 * <p>Each distinct state will be stored in one path, but when the state backend creates
	 * multiple states, they will store their files on different paths.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param paths The paths across which the local RocksDB database files will be spread.
	 */
	public void setDbStoragePaths(String... paths) {
		if (paths == null) {
			localRocksDbDirectories = null;
		}
		else if (paths.length == 0) {
			throw new IllegalArgumentException("empty paths");
		}
		else {
			File[] pp = new File[paths.length];

			for (int i = 0; i < paths.length; i++) {
				final String rawPath = paths[i];
				final String path;

				if (rawPath == null) {
					throw new IllegalArgumentException("null path");
				}
				else {
					// we need this for backwards compatibility, to allow URIs like 'file:///'...
					URI uri = null;
					try {
						uri = new Path(rawPath).toUri();
					}
					catch (Exception e) {
						// cannot parse as a path
					}

					if (uri != null && uri.getScheme() != null) {
						if ("file".equalsIgnoreCase(uri.getScheme())) {
							path = uri.getPath();
						}
						else {
							throw new IllegalArgumentException("Path " + rawPath + " has a non-local scheme");
						}
					}
					else {
						path = rawPath;
					}
				}

				pp[i] = new File(path);
				if (!pp[i].isAbsolute()) {
					throw new IllegalArgumentException("Relative paths are not supported");
				}
			}

			localRocksDbDirectories = pp;
		}
	}

	/**
	 * Gets the configured local DB storage paths, or null, if none were configured.
	 *
	 * <p>Under these directories on the TaskManager, RocksDB stores its SST files and
	 * metadata files. These directories do not need to be persistent, they can be ephermeral,
	 * meaning that they are lost on a machine failure, because state in RocksDB is persisted
	 * in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 */
	public String[] getDbStoragePaths() {
		if (localRocksDbDirectories == null) {
			return null;
		} else {
			String[] paths = new String[localRocksDbDirectories.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = localRocksDbDirectories[i].toString();
			}
			return paths;
		}
	}

	/**
	 * Gets whether incremental checkpoints are enabled for this state backend.
	 */
	public boolean isIncrementalCheckpointsEnabled() {
		return enableIncrementalCheckpointing.getOrDefault(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue());
	}

	/**
	 * Gets whether incremental checkpoints are enabled for this state backend.
	 *
	 * @deprecated enabled by default and will be removed in the future.
	 */
	@Deprecated
	public boolean isTtlCompactionFilterEnabled() {
		return enableTtlCompactionFilter.getOrDefault(TTL_COMPACT_FILTER_ENABLED.defaultValue());
	}

	/**
	 * Enable compaction filter to cleanup state with TTL.
	 *
	 * <p>Note: User can still decide in state TTL configuration in state descriptor
	 * whether the filter is active for particular state or not.
	 *
	 * @deprecated enabled by default and will be removed in the future.
	 */
	@Deprecated
	public void enableTtlCompactionFilter() {
		enableTtlCompactionFilter = TernaryBoolean.TRUE;
	}

	/**
	 * Disable compaction filter to cleanup state with TTL.
	 *
	 * <p>Note: This is an advanced option and the method should only be used
	 * when experiencing serious performance degradations during compaction in RocksDB.
	 *
	 * @deprecated enabled by default and will be removed in the future.
	 */
	@Deprecated
	public void disableTtlCompactionFilter() {
		enableTtlCompactionFilter = TernaryBoolean.FALSE;
	}

	// ------------------------------------------------------------------------
	//  Parametrize with RocksDB Options
	// ------------------------------------------------------------------------

	/**
	 * Sets the predefined options for RocksDB.
	 *
	 * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through flink-conf.yaml)
	 * or a user-defined options factory is set (via {@link #setRocksDBOptions(RocksDBOptionsFactory)}),
	 * then the options from the factory are applied on top of the here specified
	 * predefined options and customized options.
	 *
	 * @param options The options to set (must not be null).
	 */
	public void setPredefinedOptions(@Nonnull PredefinedOptions options) {
		predefinedOptions = checkNotNull(options);
	}

	/**
	 * Gets the currently set predefined options for RocksDB.
	 * The default options (if nothing was set via {@link #setPredefinedOptions(PredefinedOptions)})
	 * are {@link PredefinedOptions#DEFAULT}.
	 *
	 * <p>If user-configured options within {@link RocksDBConfigurableOptions} is set (through flink-conf.yaml)
	 * of a user-defined options factory is set (via {@link #setRocksDBOptions(RocksDBOptionsFactory)}),
	 * then the options from the factory are applied on top of the predefined and customized options.
	 *
	 * @return The currently set predefined options for RocksDB.
	 */
	@VisibleForTesting
	public PredefinedOptions getPredefinedOptions() {
		if (predefinedOptions == null) {
			predefinedOptions = PredefinedOptions.DEFAULT;
		}
		return predefinedOptions;
	}

	/**
	 * Sets {@link org.rocksdb.Options} for the RocksDB instances.
	 * Because the options are not serializable and hold native code references,
	 * they must be specified through a factory.
	 *
	 * <p>The options created by the factory here are applied on top of the pre-defined
	 * options profile selected via {@link #setPredefinedOptions(PredefinedOptions)}.
	 * If the pre-defined options profile is the default
	 * ({@link PredefinedOptions#DEFAULT}), then the factory fully controls the RocksDB
	 * options.
	 *
	 * @param optionsFactory The options factory that lazily creates the RocksDB options.
	 */
	public void setRocksDBOptions(RocksDBOptionsFactory optionsFactory) {
		this.rocksDbOptionsFactory = optionsFactory;
	}

	/**
	 * Gets {@link org.rocksdb.Options} for the RocksDB instances.
	 *
	 * <p>The options created by the factory here are applied on top of the pre-defined
	 * options profile selected via {@link #setPredefinedOptions(PredefinedOptions)}.
	 * If the pre-defined options profile is the default
	 * ({@link PredefinedOptions#DEFAULT}), then the factory fully controls the RocksDB options.
	 */
	@Nullable
	public RocksDBOptionsFactory getRocksDBOptions() {
		return rocksDbOptionsFactory;
	}

	/**
	 * The options factory supplied here was prone to resource leaks, because it did not have a way
	 * to register native handles / objects that need to be disposed when the state backend is closed.
	 *
	 * @deprecated Use {@link #setRocksDBOptions(RocksDBOptionsFactory)} instead.
	 */
	@Deprecated
	public void setOptions(OptionsFactory optionsFactory) {
		this.rocksDbOptionsFactory = optionsFactory instanceof RocksDBOptionsFactory
				? (RocksDBOptionsFactory) optionsFactory
				: new RocksDBOptionsFactoryAdapter(optionsFactory);
	}

	/**
	 * The options factory supplied here was prone to resource leaks, because it did not have a way
	 * to register native handles / objects that need to be disposed when the state backend is closed.
	 *
	 * @deprecated Use {@link #setRocksDBOptions(RocksDBOptionsFactory)} and {@link #getRocksDBOptions()} instead.
	 */
	@Deprecated
	public OptionsFactory getOptions() {
		return RocksDBOptionsFactoryAdapter.unwrapIfAdapter(rocksDbOptionsFactory);
	}

	/**
	 * Gets the number of threads used to transfer files while snapshotting/restoring.
	 */
	public int getNumberOfTransferThreads() {
		return numberOfTransferThreads == UNDEFINED_NUMBER_OF_TRANSFER_THREADS ?
			CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue() : numberOfTransferThreads;
	}

	/**
	 * Sets the number of threads used to transfer files while snapshotting/restoring.
	 *
	 * @param numberOfTransferThreads The number of threads used to transfer files while snapshotting/restoring.
	 */
	public void setNumberOfTransferThreads(int numberOfTransferThreads) {
		Preconditions.checkArgument(numberOfTransferThreads > 0,
			"The number of threads used to transfer files in RocksDBStateBackend should be greater than zero.");
		this.numberOfTransferThreads = numberOfTransferThreads;
	}

	/**
	 * @deprecated Typo in method name. Use {@link #getNumberOfTransferThreads} instead.
	 */
	@Deprecated
	public int getNumberOfTransferingThreads() {
		return getNumberOfTransferThreads();
	}

	/**
	 * @deprecated Typo in method name. Use {@link #setNumberOfTransferThreads(int)} instead.
	 */
	@Deprecated
	public void setNumberOfTransferingThreads(int numberOfTransferingThreads) {
		setNumberOfTransferThreads(numberOfTransferingThreads);
	}

	/**
	 * Gets the max batch size will be used in {@link RocksDBWriteBatchWrapper}.
	 */
	public long getWriteBatchSize() {
		return writeBatchSize == UNDEFINED_WRITE_BATCH_SIZE ?
			WRITE_BATCH_SIZE.defaultValue().getBytes() : writeBatchSize;
	}

	/**
	 * Sets the max batch size will be used in {@link RocksDBWriteBatchWrapper},
	 * no positive value will disable memory size controller, just use item count controller.
	 * @param writeBatchSize The size will used to be used in {@link RocksDBWriteBatchWrapper}.
	 */
	public void setWriteBatchSize(long writeBatchSize) {
		checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
		this.writeBatchSize = writeBatchSize;
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	@VisibleForTesting
	RocksDBResourceContainer createOptionsAndResourceContainer() {
		return createOptionsAndResourceContainer(null);
	}

	@VisibleForTesting
	private RocksDBResourceContainer createOptionsAndResourceContainer(
		@Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources) {

		return new RocksDBResourceContainer(
			predefinedOptions != null ? predefinedOptions : PredefinedOptions.DEFAULT,
			rocksDbOptionsFactory,
			sharedResources);
	}

	@Override
	public String toString() {
		return "RocksDBStateBackend{" +
				"checkpointStreamBackend=" + checkpointStreamBackend +
				", localRocksDbDirectories=" + Arrays.toString(localRocksDbDirectories) +
				", enableIncrementalCheckpointing=" + enableIncrementalCheckpointing +
				", numberOfTransferThreads=" + numberOfTransferThreads +
				", writeBatchSize=" + writeBatchSize +
				'}';
	}

	// ------------------------------------------------------------------------
	//  static library loading utilities
	// ------------------------------------------------------------------------

	@VisibleForTesting
	static void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
		synchronized (RocksDBStateBackend.class) {
			if (!rocksDbInitialized) {

				final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
				LOG.info("Attempting to load RocksDB native library and store it under '{}'", tempDirParent);

				Throwable lastException = null;
				for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
					File rocksLibFolder = null;
					try {
						// when multiple instances of this class and RocksDB exist in different
						// class loaders, then we can see the following exception:
						// "java.lang.UnsatisfiedLinkError: Native Library /path/to/temp/dir/librocksdbjni-linux64.so
						// already loaded in another class loader"

						// to avoid that, we need to add a random element to the library file path
						// (I know, seems like an unnecessary hack, since the JVM obviously can handle multiple
						//  instances of the same JNI library being loaded in different class loaders, but
						//  apparently not when coming from the same file path, so there we go)

						rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());

						// make sure the temp path exists
						LOG.debug("Attempting to create RocksDB native library folder {}", rocksLibFolder);
						// noinspection ResultOfMethodCallIgnored
						rocksLibFolder.mkdirs();

						// explicitly load the JNI dependency if it has not been loaded before
						NativeLibraryLoader.getInstance().loadLibrary(rocksLibFolder.getAbsolutePath());

						// this initialization here should validate that the loading succeeded
						RocksDB.loadLibrary();

						// seems to have worked
						LOG.info("Successfully loaded RocksDB native library");
						rocksDbInitialized = true;
						return;
					}
					catch (Throwable t) {
						lastException = t;
						LOG.debug("RocksDB JNI library loading attempt {} failed", attempt, t);

						// try to force RocksDB to attempt reloading the library
						try {
							resetRocksDBLoadedFlag();
						} catch (Throwable tt) {
							LOG.debug("Failed to reset 'initialized' flag in RocksDB native code loader", tt);
						}

						FileUtils.deleteDirectoryQuietly(rocksLibFolder);
					}
				}

				throw new IOException("Could not load the native RocksDB library", lastException);
			}
		}
	}

	@VisibleForTesting
	static void resetRocksDBLoadedFlag() throws Exception {
		final Field initField = org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
		initField.setAccessible(true);
		initField.setBoolean(null, false);
	}
}
