package com.ktech.repertoire.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * This is a special aggregator function that aggregates {@link Onboarding}
 * events using an {@link AgreementAccumulator}.
 */
public class AggregatorFunction
    implements AggregateFunction<Onboarding, AgreementAccumulator, Agreement> {
  @Override
  public AgreementAccumulator createAccumulator() {
    return new AgreementAccumulator();
  }

  @Override
  public AgreementAccumulator add(Onboarding onboarding,
                                  AgreementAccumulator agreementAccumulator) {
    agreementAccumulator.addOnboarding(onboarding);
    return agreementAccumulator;
  }

  @Override
  public Agreement getResult(AgreementAccumulator agreementAccumulator) {
    return agreementAccumulator.reduce();
  }

  @Override
  public AgreementAccumulator merge(AgreementAccumulator acc1,
                                    AgreementAccumulator acc2) {
    acc1.addAllOnboardings(acc2.getOnboardings());
    return acc1;
  }
}
