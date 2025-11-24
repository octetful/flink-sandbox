package com.ktech.repertoire.streaming;

import java.io.IOException;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * This is a very special custom trigger that only flushes when the parts
 * of an agreement in a given pane of a global window reaches the specified
 * total parts.
 */
public class OnboardingCompletedTrigger extends Trigger<Onboarding, GlobalWindow> {

  private final ValueStateDescriptor<Integer> countDescriptor;
  private final ValueStateDescriptor<Integer> expectedTotalDescriptor;

  public OnboardingCompletedTrigger() {
    countDescriptor = new ValueStateDescriptor<>("count", Integer.class);
    expectedTotalDescriptor = new ValueStateDescriptor<>("total", Integer.class);
  }

  @Override
  public TriggerResult onElement(Onboarding onboarding, long l, GlobalWindow globalWindow,
                                 TriggerContext triggerContext) throws Exception {
    // This could lead to an edge case bug condition: TBD
    updateStateOnNewEvent(triggerContext, onboarding);

    // When we have received all the expected parts, fire!
    return checkEvaluationCriteria(triggerContext);
  }

  private TriggerResult checkEvaluationCriteria(TriggerContext triggerContext) throws IOException {
    if ( getCountValue(triggerContext) >= getExpectedTotalCountValue(triggerContext) ) {
      return TriggerResult.FIRE_AND_PURGE;
    }

    // else, move on...
    return TriggerResult.CONTINUE;
  }

  private void updateStateOnNewEvent(TriggerContext triggerContext, Onboarding onboarding) throws IOException {
    if ( getExpectedTotalCountValue(triggerContext) != onboarding.getTotalParts() ) {
      updateTotalExpectedCount(triggerContext, onboarding.getTotalParts());
    }

    incrementCount(triggerContext);
  }

  private Integer getCountValue(TriggerContext triggerContext) throws IOException {
    return triggerContext.getPartitionedState(countDescriptor).value();
  }

  private Integer getExpectedTotalCountValue(TriggerContext triggerContext) throws IOException {
    return triggerContext.getPartitionedState(expectedTotalDescriptor).value();
  }

  private void incrementCount(TriggerContext triggerContext) throws IOException {
    var count = triggerContext.getPartitionedState(countDescriptor);
    count.update(count.value() + 1);
  }

  private void updateTotalExpectedCount(TriggerContext triggerContext, Integer totalCount)
      throws IOException {
    var expectedTotalCount = triggerContext.getPartitionedState(expectedTotalDescriptor);
    expectedTotalCount.update(totalCount);
  }

  @Override
  public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow,
                                        TriggerContext triggerContext) throws Exception {

    return checkEvaluationCriteria(triggerContext);
  }

  @Override
  public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext)
      throws Exception {
    return checkEvaluationCriteria(triggerContext);
  }

  @Override
  public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
    triggerContext.getPartitionedState(expectedTotalDescriptor).clear();
    triggerContext.getPartitionedState(expectedTotalDescriptor).clear();
  }
}
