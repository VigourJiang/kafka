/**
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

package kafka.coordinator

import java.util

import kafka.utils.nonthreadsafe

import scala.collection.Map


case class MemberSummary(memberId: String,
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
// jfq, 一个MemberMetadata对象对应了一个consumer。
@nonthreadsafe
private[coordinator] class MemberMetadata(val memberId: String,
                                          val groupId: String,
                                          val clientId: String,
                                          val clientHost: String,
                                          val rebalanceTimeoutMs: Int,
                                          val sessionTimeoutMs: Int,
                                          val protocolType: String,
                                          // jfq, (protocolName, protocolMetadata)
                                          var supportedProtocols: List[(String, Array[Byte])]) {

  var assignment: Array[Byte] = Array.empty[Byte]
  // jfq, when the group is in the prepare-rebalance state,
  // jfq, its rebalance callback will be kept in the metadata if the
  // jfq, member has sent the join group request
  // jfq, 背景知识：
  // jfq, 如果当前group处于PreparingRebalance状态，所有来自于member的JoinGroup请求，
  // jfq, 都会被挂起来，一直等到接收到了所有预期的member的JoinGroup请求。
  // jfq, 至于哪些是预期的member，目前的理解为：如果对于老的group，预期的member为所有Rebalance之前已经存在的member。
  // jfq, 参见DelayedJoin类
  var awaitingJoinCallback: JoinGroupResult => Unit = null

  // jfq, when the group is in the awaiting-sync state, its sync callback
  // jfq, is kept in metadata until the leader provides the group assignment
  // jfq, and the group transitions to stable
  // jfq, 背景知识：
  // jfq, 如果当前group处于AwaitingSync状态，所有来自于follower member的SyncGroup请求，
  // jfq, 都会被挂起来，直到接收到了leader member的SyncGroup请求（leader member的SyncGroup请求包含assignment信息）
  var awaitingSyncCallback: (Array[Byte], Short) => Unit = null

  var latestHeartbeat: Long = -1
  var isLeaving: Boolean = false

  def protocols = supportedProtocols.map(_._1).toSet

  /**
   * Get metadata corresponding to the provided protocol.
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  // jfq, 检查支持的protocol的数量、名字、metadata完全相同
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- 0 until protocols.size) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  // jfq, 返回supportedProtocols中第一个处于candidates中的protocol。
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString = {
    "[%s,%s,%s,%d]".format(memberId, clientId, clientHost, sessionTimeoutMs)
  }
}
