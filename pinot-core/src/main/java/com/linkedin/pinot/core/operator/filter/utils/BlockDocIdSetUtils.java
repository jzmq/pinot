package com.linkedin.pinot.core.operator.filter.utils;

import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.BitmapAndBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.BitmapDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.BitmapOrBlockDocIdSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by dengqiang on 9/21/15.
 */
public class BlockDocIdSetUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockDocIdSetUtils.class);

  public static boolean isBitmapSet(FilterBlockDocIdSet srcSet) {
    return srcSet instanceof BitmapDocIdSet || srcSet instanceof BitmapAndBlockDocIdSet || srcSet instanceof
            BitmapOrBlockDocIdSet;
  }
}
