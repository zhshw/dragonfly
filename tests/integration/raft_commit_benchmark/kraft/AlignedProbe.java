// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package org.apache.kafka.raft;

/** Timestamps from the Raft thread, indexed by record offset in a fresh benchmark log. */
public final class AlignedProbe {
  private static final long[] APPEND = new long[4096];
  private static final long[] HW = new long[4096];
  private static final int[] EPOCH = new int[4096];
  private static long previous;

  private AlignedProbe() {}

  public static void append(long first, long last, int epoch, long nanos) {
    for (long offset = first; offset <= last; offset++) {
      APPEND[(int) offset] = nanos;
      EPOCH[(int) offset] = epoch;
    }
  }

  public static void advance(long high, long nanos) {
    while (previous < high) {
      HW[(int) previous++] = nanos;
    }
  }

  public static long appendNs(long offset) {
    return APPEND[(int) offset];
  }

  public static long hwNs(long offset) {
    return HW[(int) offset];
  }

  public static int epoch(long offset) {
    return EPOCH[(int) offset];
  }
}
