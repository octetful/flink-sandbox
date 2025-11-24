package com.ktech.repertoire.streaming;

import java.util.ArrayList;
import java.util.List;

/**
 * Special accumulator used for aggregating {@link Onboarding}
 * to {@link Agreement}.
 */
public class AgreementAccumulator {
  private final List<Onboarding> onboardings;

  public AgreementAccumulator() {
    onboardings = new ArrayList<>();
  }

  public void addOnboarding(Onboarding onboarding) {
    onboardings.add(onboarding);
  }

  public void addAllOnboardings(List<Onboarding> onboardings) {
    this.onboardings.addAll(onboardings);
  }

  public List<Onboarding> getOnboardings() {
    return onboardings;
  }

  public Agreement reduce() {
    Agreement agreement = new Agreement();
    if ((onboardings.isEmpty())) {
      agreement.setCorrelationId("");
      agreement.setScheduleIds(new ArrayList<>());
    } else {
      agreement.setCorrelationId(onboardings.get(0).getCorrelationId());
      agreement.setScheduleIds(
          onboardings.stream().map(Onboarding::getScheduleId).toList());
    }
    return agreement;
  }
}
