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
package org.apache.kafka.common.protocol;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    // jfq, client发送给leader broker，产生新消息
    PRODUCE(0, "Produce"),
    // jfq, client发送给leader broker，获取消息；follower broker也会给leader发送该消息。
    FETCH(1, "Fetch"),
    // jfq, client发送给leader broker，根据时间戳查询offset；follower broker也会给leader发送该消息。
    LIST_OFFSETS(2, "Offsets"),
    // jfq, client发送给任意broker，要求获取cluster的topic、partition等全局信息
    METADATA(3, "Metadata"),

    // jfq, controller <-> broker
    LEADER_AND_ISR(4, "LeaderAndIsr"),
    // jfq, controller <-> broker
    STOP_REPLICA(5, "StopReplica"),
    // jfq, controller <-> broker
    UPDATE_METADATA_KEY(6, "UpdateMetadata"),

    CONTROLLED_SHUTDOWN_KEY(7, "ControlledShutdown"),

    // jfq, Consumer向Coordinator提交Offset
    OFFSET_COMMIT(8, "OffsetCommit"),
    // jfq, Consumer从Coordinator获取最近一次提交的Offset
    OFFSET_FETCH(9, "OffsetFetch"),

    // jfq, Consumer向Node发送GROUP_COORDINATOR请求，查询自己的Coordinator是哪个Broker
    GROUP_COORDINATOR(10, "GroupCoordinator"),
    // jfq, Consumer向Coordinator发送的请求，用来加入某个ConsumerGroup
    JOIN_GROUP(11, "JoinGroup"),
    // jfq, Consumer向Coordinator发送的HeartBeat
    HEARTBEAT(12, "Heartbeat"),
    // jfq, Consumer向Coordinator主动请求离开某个consumer Group
    LEAVE_GROUP(13, "LeaveGroup"),
    // jfq, Consumer向Coordinator发送的Sync_Group命令，leader Consumer和follower Consumer发送的内容不同。
    SYNC_GROUP(14, "SyncGroup"),

    // jfq, Admin工具向Broker发送的请求
    DESCRIBE_GROUPS(15, "DescribeGroups"),
    // jfq, Admin工具向Broker发送的请求
    LIST_GROUPS(16, "ListGroups"),

    SASL_HANDSHAKE(17, "SaslHandshake"),
    API_VERSIONS(18, "ApiVersions"),
    CREATE_TOPICS(19, "CreateTopics"),
    DELETE_TOPICS(20, "DeleteTopics");

    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values())
            maxKey = Math.max(maxKey, key.id);
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values())
            idToType[key.id] = key;
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    private ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static ApiKeys forId(int id) {
        if (id < MIN_API_KEY || id > MAX_API_KEY)
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`, it should be between `%s` " +
                    "and `%s` (inclusive)", id, MIN_API_KEY, MAX_API_KEY));
        return ID_TO_TYPE[id];
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append(key.name);
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

}
