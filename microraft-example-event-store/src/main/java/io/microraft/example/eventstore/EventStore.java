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

import io.microraft.statemachine.StateMachine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Event Store which supports appending and reading events.
 *
 * @author Milan Savic
 */
public class EventStore implements StateMachine {

    private static final int INSTALL_SNAPSHOT_MAX_EVENTS_COUNT = 10;

    private final List<Object> events = new ArrayList<>();

    @Override
    public Object runOperation(long commitIndex, @Nonnull Object operation) {
        if (operation instanceof NewTermOperation) {
            return null;
        } else if (operation instanceof AppendEventOperation) {
            events.add(((AppendEventOperation) operation).event);
            return true;
        } else if (operation instanceof ReadEventsOperation) {
            return events.subList(((ReadEventsOperation) operation).startSequence, events.size());
        }
        throw new IllegalArgumentException("Invalid operation: " + operation + " at commit index: " + commitIndex);
    }

    @Override
    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        List<Object> chunk = new ArrayList<>(INSTALL_SNAPSHOT_MAX_EVENTS_COUNT);
        for (Object event : events) {
            chunk.add(event);
            if (chunk.size() == INSTALL_SNAPSHOT_MAX_EVENTS_COUNT) {
                snapshotChunkConsumer.accept(new EventStoreChunk(chunk));
                chunk.clear();
            }
        }
        if (chunk.size() > 0) {
            snapshotChunkConsumer.accept(new EventStoreChunk(chunk));
        }
    }

    @Override
    public void installSnapshot(long commitIndex, @Nonnull List<Object> snapshotChunks) {
        this.events.clear();
        snapshotChunks.stream()
                      .map(chunk -> ((EventStoreChunk) chunk).events)
                      .forEach(this.events::addAll);
    }

    @Nullable
    @Override
    public Object getNewTermOperation() {
        return new NewTermOperation();
    }

    /**
     * Creates a new {@link EventStoreOperation} to append a given {@code event}.
     *
     * @param event an event to be appended
     * @return a new instance of operation
     */
    public static EventStoreOperation newAppendEventOperation(Object event) {
        return new AppendEventOperation(event);
    }

    /**
     * Creates a new {@link EventStoreOperation} to read events from event store.
     *
     * @param startSequence the sequence from which the reading would start
     * @return a new instance of operation
     */
    public static ReadEventsOperation newReadEventsOperation(int startSequence) {
        return new ReadEventsOperation(startSequence);
    }

    /**
     * Marker interface for operations supported by this event store.
     */
    public interface EventStoreOperation {

    }

    private static class AppendEventOperation implements EventStoreOperation {

        final Object event;

        AppendEventOperation(Object event) {
            this.event = event;
        }

        @Override
        public String toString() {
            return "AppendEventOperation{" +
                    "event=" + event +
                    '}';
        }
    }

    private static class ReadEventsOperation implements EventStoreOperation {

        final int startSequence;

        ReadEventsOperation(int startSequence) {
            this.startSequence = startSequence;
        }

        @Override
        public String toString() {
            return "ReadEventsOperation{" +
                    "startSequence=" + startSequence +
                    '}';
        }
    }

    private static class NewTermOperation {

    }

    private static class EventStoreChunk {

        private final List<Object> events;

        private EventStoreChunk(List<Object> events) {
            this.events = new ArrayList<>(events);
        }

        @Override
        public String toString() {
            return "EventStoreChunk{" +
                    "events=" + events +
                    '}';
        }
    }
}
