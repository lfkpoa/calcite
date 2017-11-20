/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;

/**
 * Enumerates the types of Fetch/Offset used in databases
 */
public enum FetchOffsetType {

  NONE(false, false),

  LIMIT(true, false) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      if (fetch != null && frameType == SqlWriter.FrameTypeEnum.ORDER_BY) {
        writer.newlineAndIndent();
        final Frame fetchFrame = writer.startList(FrameTypeEnum.FETCH);
        writer.keyword("LIMIT");
        fetch.unparse(writer, -1, -1);
        writer.endList(fetchFrame);
      }
    }
  },

  LIMIT_OFFSET(true, true) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      LIMIT.unparse(frameType, writer, fetch, null);
      if (offset != null && frameType == SqlWriter.FrameTypeEnum.ORDER_BY) {
        writer.newlineAndIndent();
        final Frame offsetFrame = writer.startList(FrameTypeEnum.OFFSET);
        writer.keyword("OFFSET");
        offset.unparse(writer, -1, -1);
        writer.endList(offsetFrame);
      }
    }
  },

  OFFSET_LIMIT(true, true) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      if (offset == null) {
        LIMIT.unparse(frameType, writer, fetch, null);
        return;
      }
      if (frameType == SqlWriter.FrameTypeEnum.ORDER_BY) {
        writer.newlineAndIndent();
        final Frame offsetFrame = writer.startList(FrameTypeEnum.OFFSET);
        writer.keyword("LIMIT");
        offset.unparse(writer, -1, -1);
        writer.sep(",", true);
        fetch.unparse(writer, -1, -1);
        writer.endList(offsetFrame);
      }
    }
  },

  FIRST(true, false) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      if (fetch != null && frameType == SqlWriter.FrameTypeEnum.SELECT) {
        final Frame fetchFrame = writer.startList(FrameTypeEnum.FETCH);
        writer.keyword("FIRST");
        fetch.unparse(writer, -1, -1);
        writer.endList(fetchFrame);
      }
    }
  },

  TOP(true, false) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      if (fetch != null && frameType == SqlWriter.FrameTypeEnum.SELECT) {
        final Frame fetchFrame = writer.startList(FrameTypeEnum.FETCH);
        writer.keyword("TOP");
        fetch.unparse(writer, -1, -1);
        writer.endList(fetchFrame);
      }
    }
  },

  FETCH(true, false) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      if (fetch != null && frameType == SqlWriter.FrameTypeEnum.ORDER_BY) {
        writer.newlineAndIndent();
        final Frame fetchFrame = writer.startList(FrameTypeEnum.FETCH);
        writer.keyword("FETCH");
        writer.keyword("FIRST");
        fetch.unparse(writer, -1, -1);
        writer.keyword("ROWS");
        writer.keyword("ONLY");
        writer.endList(fetchFrame);
      }
    }
  },

  FETCH_OFFSET(true, true) {
    @Override public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer,
        SqlNode fetch, SqlNode offset) {
      if (frameType != SqlWriter.FrameTypeEnum.ORDER_BY) {
        return;
      }
      if (offset != null) {
        writer.newlineAndIndent();
        final Frame offsetFrame =
            writer.startList(FrameTypeEnum.OFFSET);
        writer.keyword("OFFSET");
        offset.unparse(writer, -1, -1);
        writer.keyword("ROWS");
        writer.endList(offsetFrame);
      }
      if (fetch != null) {
        writer.newlineAndIndent();
        final Frame fetchFrame = writer.startList(FrameTypeEnum.FETCH);
        writer.keyword("FETCH");
        writer.keyword("NEXT");
        fetch.unparse(writer, -1, -1);
        writer.keyword("ROWS");
        writer.keyword("ONLY");
        writer.endList(fetchFrame);
      }
    }
  };

  private final boolean canFetch;
  private final boolean canOffset;

  FetchOffsetType(boolean canFetch, boolean canOffset) {
    this.canFetch = canFetch;
    this.canOffset = canOffset;
  }

  // Writes the Fetch/Offset clause
  public void unparse(SqlWriter.FrameTypeEnum frameType, SqlWriter writer, SqlNode fetch,
      SqlNode offset) {
  }

  public boolean supportsFetch() {
    return canFetch;
  }

  public boolean supportsOffset() {
    return canOffset;
  }
}

// End FetchOffsetType.java
