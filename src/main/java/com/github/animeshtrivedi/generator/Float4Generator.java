package com.github.animeshtrivedi.generator;

import com.github.animeshtrivedi.benchmark.Configuration;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.types.Types;

import java.nio.channels.WritableByteChannel;

public class Float4Generator extends ArrowDataGenerator {
  int totalFloat4s;
  private long totalRows;

  public Float4Generator(WritableByteChannel channel) {
    super(channel);
    try {
      super.makeArrowSchema("", Types.MinorType.FLOAT4);
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.totalFloat4s = 0;
    this.totalRows = 0;
  }

  @Override
  int fillBatch(int startIndex, int endIndex, FieldVector vector){
    Float4Vector float4Vector = (Float4Vector) vector;
    for (int i = startIndex; i < endIndex; i++) {
      float4Vector.setSafe(i, 1, (float)i);
    }
    if(Configuration.generateOneNull) {
      // for debugging mark one NULL
      float4Vector.setSafe(endIndex / 2, 0, (float)0);
      this.totalFloat4s+=(endIndex - startIndex - 1);
    }else{
      this.totalFloat4s+=(endIndex - startIndex);
    }

    totalRows+=(endIndex - startIndex);
    return (endIndex - startIndex);
  }

  public String toString() {
    return "Float4Generator" + super.toString();
  }

  @Override
  public long totalInts() {
    return 0;
  }

  @Override
  public long totalLongs() {
    return 0;
  }

  @Override
  public long totalFloat8() {
    return 0;
  }

  @Override
  public long totalFloat4() {
    return totalFloat4s;
  }

  @Override
  public long totalBinary() {
    return 0;
  }

  @Override
  public long totalBinarySize() {
    return 0;
  }

  @Override
  public long totalRows() {
    return totalRows;
  }

  @Override
  public double getChecksum() {
    return 0;
  }

  @Override
  public long getRunTimeinNS() {
    return 0;
  }
}

