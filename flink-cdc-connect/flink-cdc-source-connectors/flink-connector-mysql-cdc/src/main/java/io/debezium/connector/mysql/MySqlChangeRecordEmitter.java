/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import java.io.Serializable;

/**
 * Copied from Debezium project(1.9.8.final) to support parallel processing.
 *
 * <p>Line 53: Change visibility of {@link #getOperation()} to public.
 *
 * <p>Line 58: Change visibility of {@link #getOldColumnValues()} to public.
 *
 * <p>Line 69: Change visibility of {@link #getNewColumnValues()} to public.
 */
public class MySqlChangeRecordEmitter extends RelationalChangeRecordEmitter<MySqlPartition> {

    private final Envelope.Operation operation;
    private final OffsetContext offset;
    private final Object[] before;
    private final Object[] after;

    public MySqlChangeRecordEmitter(
            MySqlPartition partition,
            OffsetContext offset,
            Clock clock,
            Envelope.Operation operation,
            Serializable[] before,
            Serializable[] after) {
        super(partition, offset, clock);
        this.offset = offset;
        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    @Override
    public OffsetContext getOffset() {
        return offset;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Object[] getOldColumnValues() {
        return before != null ? before : null;
    }

    @Override
    public Object[] getNewColumnValues() {
        return after != null ? after : null;
    }
}
