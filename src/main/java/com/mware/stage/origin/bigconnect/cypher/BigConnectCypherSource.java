package com.mware.stage.origin.bigconnect.cypher;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class BigConnectCypherSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(BigConnectCypherSource.class);

  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    // Clean up any open resources.
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(Map<String, String> offsets, int maxBatchSize) throws StageException {
    //TODO
  }
}
