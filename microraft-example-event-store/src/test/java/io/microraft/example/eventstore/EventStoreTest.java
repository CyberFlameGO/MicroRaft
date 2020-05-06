/*
 * Copyright (c) 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.example.eventstore;

import io.microraft.Ordered;
import io.microraft.QueryPolicy;
import io.microraft.RaftConfig;
import io.microraft.RaftNode;
import io.microraft.report.RaftLogStats;
import io.microraft.statemachine.StateMachine;
import org.junit.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Tests for clustered Event Store.
 *
 * @author Milan Savic
 */
public class EventStoreTest extends BaseLocalTest {

    private static final int COMMIT_COUNT_TO_TAKE_SNAPSHOT = 100;

    @Override
    protected RaftConfig getConfig() {
        return RaftConfig.newBuilder()
                         .setCommitCountToTakeSnapshot(COMMIT_COUNT_TO_TAKE_SNAPSHOT)
                         .build();
    }

    @Override
    protected StateMachine createStateMachine() {
        return new EventStore();
    }

    @Test
    public void testReplication() {
        RaftNode leader = waitUntilLeaderElected();

        CompletableFuture<Ordered<Object>> replication1 = leader.replicate(EventStore.newAppendEventOperation("event1"));
        CompletableFuture<Ordered<Object>> replication2 = leader.replicate(EventStore.newAppendEventOperation("event2"));
        CompletableFuture<Ordered<Object>> replication3 = leader.replicate(EventStore.newAppendEventOperation("event3"));
        CompletableFuture<Ordered<Object>> replication4 = leader.replicate(EventStore.newAppendEventOperation("event4"));

        CompletableFuture.allOf(replication1, replication2, replication3, replication4).join();

        List<Object> events = leader.<List<Object>>replicate(EventStore.newReadEventsOperation(0)).join().getResult();
        assertEquals(Arrays.asList("event1", "event2", "event3", "event4"), events);

        leader.replicate(EventStore.newAppendEventOperation("event5")).join();

        events = leader.<List<Object>>replicate(EventStore.newReadEventsOperation(2)).join().getResult();
        assertEquals(Arrays.asList("event3", "event4", "event5"), events);
    }

    @Test
    public void testInstallSnapshot() {
        RaftNode leader = waitUntilLeaderElected();
        RaftNode aFollower = getAnyNodeExcept(leader.getLocalEndpoint());

        IntStream.rangeClosed(1, 5)
                 .mapToObj(i -> "event" + i)
                 .map(e -> leader.replicate(EventStore.newAppendEventOperation(e)))
                 .forEach(CompletableFuture::join);

        disconnect(leader.getLocalEndpoint(), aFollower.getLocalEndpoint());

        IntStream.rangeClosed(5, COMMIT_COUNT_TO_TAKE_SNAPSHOT)
                 .mapToObj(i -> "event" + i)
                 .map(e -> leader.replicate(EventStore.newAppendEventOperation(e)))
                 .forEach(CompletableFuture::join);

        assertThat(getRaftLogStats(leader).getTakeSnapshotCount()).isEqualTo(1);

        connect(leader.getLocalEndpoint(), aFollower.getLocalEndpoint());

        eventually(() -> {
            RaftLogStats logStats = getRaftLogStats(aFollower);
            assertThat(logStats.getInstallSnapshotCount()).isEqualTo(1);
            assertThat(logStats.getCommitIndex()).isEqualTo(getRaftLogStats(leader).getCommitIndex());
        });

        eventually(() -> assertThat(getRaftLogStats(aFollower).getInstallSnapshotCount()).isEqualTo(1));

        Ordered<List<Object>> leaderQueryResult = leader.<List<Object>>query(EventStore.newReadEventsOperation(0),
                                                                             QueryPolicy.ANY_LOCAL, 0).join();

        Ordered<List<Object>> aFollowerQueryResult = aFollower.<List<Object>>query(EventStore.newReadEventsOperation(0),
                                                                                   QueryPolicy.ANY_LOCAL, 0).join();

        assertThat(aFollowerQueryResult.getCommitIndex()).isEqualTo(leaderQueryResult.getCommitIndex());
        assertThat(aFollowerQueryResult.getResult()).isEqualTo(leaderQueryResult.getResult());
    }

    private RaftLogStats getRaftLogStats(RaftNode leader) {
        return leader.getReport().join().getResult().getLog();
    }
}
