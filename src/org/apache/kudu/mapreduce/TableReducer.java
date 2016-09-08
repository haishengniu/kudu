package org.apache.kudu.mapreduce;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.client.Operation;
import org.apache.hadoop.mapreduce.Reducer;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TableReducer<KEYIN, VALUEIN, KEYOUT>
    extends Reducer<KEYIN, VALUEIN, KEYOUT, Operation> {
}