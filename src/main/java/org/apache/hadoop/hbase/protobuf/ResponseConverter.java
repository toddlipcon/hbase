/**
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
package org.apache.hadoop.hbase.protobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.ByteString;

/**
 * Helper utility to build protocol buffer responses,
 * or retrieve data from protocol buffer responses.
 */
public final class ResponseConverter {

  private ResponseConverter() {
  }

// Start utilities for Client

  /**
   * Get the client Results from a protocol buffer ScanResponse
   *
   * @param response the protocol buffer ScanResponse
   * @return the client Results in the response
   */
  public static Result[] getResults(final ScanResponse response) {
    if (response == null) return null;
    int count = response.getResultCount();
    Result[] results = new Result[count];
    for (int i = 0; i < count; i++) {
      results[i] = ProtobufUtil.toResult(response.getResult(i));
    }
    return results;
  }

  /**
   * Get the results from a protocol buffer MultiResponse
   *
   * @param proto the protocol buffer MultiResponse to convert
   * @return the results in the MultiResponse
   * @throws IOException
   */
  public static List<Object> getResults(
      final ClientProtos.MultiResponse proto) throws IOException {
    List<Object> results = new ArrayList<Object>();
    List<ActionResult> resultList = proto.getResultList();
    for (int i = 0, n = resultList.size(); i < n; i++) {
      ActionResult result = resultList.get(i);
      if (result.hasException()) {
        results.add(ProtobufUtil.toException(result.getException()));
      } else if (result.hasValue()) {
        Object value = ProtobufUtil.toObject(result.getValue());
        if (value instanceof ClientProtos.Result) {
          results.add(ProtobufUtil.toResult((ClientProtos.Result)value));
        } else {
          results.add(value);
        }
      } else {
        results.add(new Result());
      }
    }
    return results;
  }

  /**
   * Wrap a throwable to an action result.
   *
   * @param t
   * @return an action result
   */
  public static ActionResult buildActionResult(final Throwable t) {
    ActionResult.Builder builder = ActionResult.newBuilder();
    NameBytesPair.Builder parameterBuilder = NameBytesPair.newBuilder();
    parameterBuilder.setName(t.getClass().getName());
    parameterBuilder.setValue(
      ByteString.copyFromUtf8(StringUtils.stringifyException(t)));
    builder.setException(parameterBuilder.build());
    return builder.build();
  }

// End utilities for Client
// Start utilities for Admin

  /**
   * Get the list of regions to flush from a RollLogWriterResponse
   *
   * @param proto the RollLogWriterResponse
   * @return the the list of regions to flush
   */
  public static byte[][] getRegions(final RollWALWriterResponse proto) {
    if (proto == null || proto.getRegionToFlushCount() == 0) return null;
    List<byte[]> regions = new ArrayList<byte[]>();
    for (ByteString region: proto.getRegionToFlushList()) {
      regions.add(region.toByteArray());
    }
    return (byte[][])regions.toArray();
  }

  /**
   * Get the list of region info from a GetOnlineRegionResponse
   *
   * @param proto the GetOnlineRegionResponse
   * @return the list of region info
   */
  public static List<HRegionInfo> getRegionInfos
      (final GetOnlineRegionResponse proto) {
    if (proto == null || proto.getRegionInfoCount() == 0) return null;
    List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>();
    for (RegionInfo regionInfo: proto.getRegionInfoList()) {
      regionInfos.add(ProtobufUtil.toRegionInfo(regionInfo));
    }
    return regionInfos;
  }

  /**
   * Get the region opening state from a OpenRegionResponse
   *
   * @param proto the OpenRegionResponse
   * @return the region opening state
   */
  public static RegionOpeningState getRegionOpeningState
      (final OpenRegionResponse proto) {
    if (proto == null || proto.getOpeningStateCount() != 1) return null;
    return RegionOpeningState.valueOf(
      proto.getOpeningState(0).name());
  }

  /**
   * Check if the region is closed from a CloseRegionResponse
   *
   * @param proto the CloseRegionResponse
   * @return the region close state
   */
  public static boolean isClosed
      (final CloseRegionResponse proto) {
    if (proto == null || !proto.hasClosed()) return false;
    return proto.getClosed();
  }

// End utilities for Admin
}
