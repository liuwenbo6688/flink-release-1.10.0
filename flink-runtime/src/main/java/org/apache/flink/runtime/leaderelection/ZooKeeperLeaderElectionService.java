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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;

/**
 * Leader election service for multiple JobManager. The leading JobManager is elected using
 * ZooKeeper. The current leader's address as well as its leader session ID is published via
 * ZooKeeper as well.
 *
 *   ZooKeeperLeaderElectionService除了实现LeaderElectionService以外，
 *   还实现了LeaderLatchListener，NodeCacheListener，UnhandledErrorListener三个属于curator的接口。
 *
 *   LeaderLatchListener需要实现类实现两个回调方法,
 *   当被监听的对象(此处即为该ZookeeperLeaderElectionService实例)被选为leader时，isLeader实现的逻辑会被调用,
 * 	 当失去leader位置时，notLeader会被调用。
 */
public class ZooKeeperLeaderElectionService implements LeaderElectionService, LeaderLatchListener, NodeCacheListener, UnhandledErrorListener {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionService.class);

	private final Object lock = new Object();

	/** Client to the ZooKeeper quorum. */
	private final CuratorFramework client;

	/** Curator recipe for leader election. */
	private final LeaderLatch leaderLatch;

	/** Curator recipe to watch a given ZooKeeper node for changes. */
	private final NodeCache cache;

	/** ZooKeeper path of the node which stores the current leader information. */
	private final String leaderPath;

	private volatile UUID issuedLeaderSessionID;

	private volatile UUID confirmedLeaderSessionID;

	private volatile String confirmedLeaderAddress;

	/**
	 * 选举服务回调的接口
	 */
	/** The leader contender which applies for leadership. */
	private volatile LeaderContender leaderContender;

	private volatile boolean running;

	private final ConnectionStateListener listener = new ConnectionStateListener() {
		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			handleStateChange(newState);
		}
	};

	/**
	 * Creates a ZooKeeperLeaderElectionService object.
	 *
	 * @param client Client which is connected to the ZooKeeper quorum
	 * @param latchPath ZooKeeper node path for the leader election latch
	 * @param leaderPath ZooKeeper node path for the node which stores the current leader information
	 */
	public ZooKeeperLeaderElectionService(CuratorFramework client, String latchPath, String leaderPath) {
		this.client = Preconditions.checkNotNull(client, "CuratorFramework client");
		this.leaderPath = Preconditions.checkNotNull(leaderPath, "leaderPath");

		leaderLatch = new LeaderLatch(client, latchPath);
		cache = new NodeCache(client, leaderPath);

		issuedLeaderSessionID = null;
		confirmedLeaderSessionID = null;
		confirmedLeaderAddress = null;
		leaderContender = null;

		running = false;
	}

	/**
	 * Returns the current leader session ID or null, if the contender is not the leader.
	 *
	 * @return The last leader session ID or null, if the contender is not the leader
	 */
	public UUID getLeaderSessionID() {
		return confirmedLeaderSessionID;
	}

	@Override
	public void start(LeaderContender contender) throws Exception {
		Preconditions.checkNotNull(contender, "Contender must not be null.");
		Preconditions.checkState(leaderContender == null, "Contender was already set.");

		LOG.info("Starting ZooKeeperLeaderElectionService {}.", this);

		synchronized (lock) {

			client.getUnhandledErrorListenable().addListener(this);

			/**
			 * 实际的竞争者，被选举成功之后，调用的也是leaderContender的方法进行回调
			 */
			leaderContender = contender;

			leaderLatch.addListener(this);
			leaderLatch.start();

			cache.getListenable().addListener(this);
			cache.start();

			client.getConnectionStateListenable().addListener(listener);

			running = true;
		}
	}

	@Override
	public void stop() throws Exception{
		synchronized (lock) {
			if (!running) {
				return;
			}

			running = false;
			confirmedLeaderSessionID = null;
			issuedLeaderSessionID = null;
		}

		LOG.info("Stopping ZooKeeperLeaderElectionService {}.", this);

		client.getUnhandledErrorListenable().removeListener(this);

		client.getConnectionStateListenable().removeListener(listener);

		Exception exception = null;

		try {
			cache.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderLatch.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw new Exception("Could not properly stop the ZooKeeperLeaderElectionService.", exception);
		}
	}

	@Override
	public void confirmLeadership(UUID leaderSessionID, String leaderAddress) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(
				"Confirm leader session ID {} for leader {}.",
				leaderSessionID,
				leaderAddress);
		}

		Preconditions.checkNotNull(leaderSessionID);

		if (leaderLatch.hasLeadership()) {
			// check if this is an old confirmation call
			synchronized (lock) {
				if (running) {
					if (leaderSessionID.equals(this.issuedLeaderSessionID)) {
						confirmLeaderInformation(leaderSessionID, leaderAddress);
						writeLeaderInformation();
					}
				} else {
					LOG.debug("Ignoring the leader session Id {} confirmation, since the " +
						"ZooKeeperLeaderElectionService has already been stopped.", leaderSessionID);
				}
			}
		} else {
			LOG.warn("The leader session ID {} was confirmed even though the " +
					"corresponding JobManager was not elected as the leader.", leaderSessionID);
		}
	}

	private void confirmLeaderInformation(UUID leaderSessionID, String leaderAddress) {
		confirmedLeaderSessionID = leaderSessionID;
		confirmedLeaderAddress = leaderAddress;
	}

	@Override
	public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
		return leaderLatch.hasLeadership() && leaderSessionId.equals(issuedLeaderSessionID);
	}

	/**
	 * 被选举为 leader后，调用的方法
	 */
	@Override
	public void isLeader() {
		synchronized (lock) {
			if (running) {
				/**
				 * 锁 + volatile变量 保证线程安全
				 */
				issuedLeaderSessionID = UUID.randomUUID();
				clearConfirmedLeaderInformation();

				if (LOG.isDebugEnabled()) {
					LOG.debug(
						"Grant leadership to contender {} with session ID {}.",
						leaderContender.getDescription(),
						issuedLeaderSessionID);
				}

				/**
				 *
				 */
				leaderContender.grantLeadership(issuedLeaderSessionID);
			} else {
				LOG.debug("Ignoring the grant leadership notification since the service has " +
					"already been stopped.");
			}
		}
	}

	private void clearConfirmedLeaderInformation() {
		confirmedLeaderSessionID = null;
		confirmedLeaderAddress = null;
	}

	/**
	 * 失去 leader
	 */
	@Override
	public void notLeader() {
		synchronized (lock) {
			if (running) {
				LOG.debug(
					"Revoke leadership of {} ({}@{}).",
					leaderContender.getDescription(),
					confirmedLeaderSessionID,
					confirmedLeaderAddress);

				issuedLeaderSessionID = null;
				clearConfirmedLeaderInformation();

				/**
				 *
				 */
				leaderContender.revokeLeadership();
			} else {
				LOG.debug("Ignoring the revoke leadership notification since the service " +
					"has already been stopped.");
			}
		}
	}

	@Override
	public void nodeChanged() throws Exception {
		try {
			// leaderSessionID is null if the leader contender has not yet confirmed the session ID
			if (leaderLatch.hasLeadership()) {
				synchronized (lock) {
					if (running) {
						if (LOG.isDebugEnabled()) {
							LOG.debug(
								"Leader node changed while {} is the leader with session ID {}.",
								leaderContender.getDescription(),
								confirmedLeaderSessionID);
						}

						if (confirmedLeaderSessionID != null) {
							ChildData childData = cache.getCurrentData();

							if (childData == null) {
								if (LOG.isDebugEnabled()) {
									LOG.debug(
										"Writing leader information into empty node by {}.",
										leaderContender.getDescription());
								}
								writeLeaderInformation();
							} else {
								byte[] data = childData.getData();

								if (data == null || data.length == 0) {
									// the data field seems to be empty, rewrite information
									if (LOG.isDebugEnabled()) {
										LOG.debug(
											"Writing leader information into node with empty data field by {}.",
											leaderContender.getDescription());
									}
									writeLeaderInformation();
								} else {
									ByteArrayInputStream bais = new ByteArrayInputStream(data);
									ObjectInputStream ois = new ObjectInputStream(bais);

									/**
									 * 读取到最新的leader的信息（地址和ID）
									 */
									String leaderAddress = ois.readUTF();
									UUID leaderSessionID = (UUID) ois.readObject();

									if (!leaderAddress.equals(confirmedLeaderAddress) ||
										(leaderSessionID == null || !leaderSessionID.equals(confirmedLeaderSessionID))) {
										// the data field does not correspond to the expected leader information
										if (LOG.isDebugEnabled()) {
											LOG.debug(
												"Correcting leader information by {}.",
												leaderContender.getDescription());
										}
										writeLeaderInformation();
									}
								}
							}
						}
					} else {
						LOG.debug("Ignoring node change notification since the service has already been stopped.");
					}
				}
			}
		} catch (Exception e) {
			leaderContender.handleError(new Exception("Could not handle node changed event.", e));
			throw e;
		}
	}

	/**
	 * Writes the current leader's address as well the given leader session ID to ZooKeeper.
	 */
	protected void writeLeaderInformation() {
		// this method does not have to be synchronized because the curator framework client
		// is thread-safe
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Write leader information: Leader={}, session ID={}.",
					confirmedLeaderAddress,
					confirmedLeaderSessionID);
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(confirmedLeaderAddress);
			oos.writeObject(confirmedLeaderSessionID);

			oos.close();

			boolean dataWritten = false;

			while (!dataWritten && leaderLatch.hasLeadership()) {
				Stat stat = client.checkExists().forPath(leaderPath);

				if (stat != null) {
					long owner = stat.getEphemeralOwner();
					long sessionID = client.getZookeeperClient().getZooKeeper().getSessionId();

					if (owner == sessionID) {
						try {
							client.setData().forPath(leaderPath, baos.toByteArray());

							dataWritten = true;
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime
						}
					} else {
						try {
							client.delete().forPath(leaderPath);
						} catch (KeeperException.NoNodeException noNode) {
							// node was deleted in the meantime --> try again
						}
					}
				} else {
					try {
						client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
								leaderPath,
								baos.toByteArray());

						dataWritten = true;
					} catch (KeeperException.NodeExistsException nodeExists) {
						// node has been created in the meantime --> try again
					}
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug(
					"Successfully wrote leader information: Leader={}, session ID={}.",
					confirmedLeaderAddress,
					confirmedLeaderSessionID);
			}
		} catch (Exception e) {
			leaderContender.handleError(
					new Exception("Could not write leader address and leader session ID to " +
							"ZooKeeper.", e));
		}
	}

	protected void handleStateChange(ConnectionState newState) {
		switch (newState) {
			case CONNECTED:
				LOG.debug("Connected to ZooKeeper quorum. Leader election can start.");
				break;
			case SUSPENDED:
				LOG.warn("Connection to ZooKeeper suspended. The contender " + leaderContender.getDescription()
					+ " no longer participates in the leader election.");
				break;
			case RECONNECTED:
				LOG.info("Connection to ZooKeeper was reconnected. Leader election can be restarted.");
				break;
			case LOST:
				// Maybe we have to throw an exception here to terminate the JobManager
				LOG.warn("Connection to ZooKeeper lost. The contender " + leaderContender.getDescription()
					+ " no longer participates in the leader election.");
				break;
		}
	}

	@Override
	public void unhandledError(String message, Throwable e) {
		leaderContender.handleError(new FlinkException("Unhandled error in ZooKeeperLeaderElectionService: " + message, e));
	}

	@Override
	public String toString() {
		return "ZooKeeperLeaderElectionService{" +
			"leaderPath='" + leaderPath + '\'' +
			'}';
	}
}
