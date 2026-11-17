package io.github.matabar.kafkafim.decode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * The ops a route's {@code transform} can name, each with the factory that compiles its step. The
 * op set is closed (ADR-0047): {@link #BUILT_IN} is the only registry production uses, and a new op
 * is one entry in it. Tests build their own registry with {@link #of}.
 */
public final class StepRegistry {

  /** Compiles one step from its spec. */
  @FunctionalInterface
  public interface Factory {

    /**
     * Compiles {@code spec}; called once per step when the pipeline starts.
     *
     * @throws IllegalArgumentException if the spec is not usable (a bound step has already passed
     *     the configuration validator, so this is a defensive check)
     */
    TransformStep compile(TransformStepSpec spec);
  }

  /** The production ops. */
  public static final StepRegistry BUILT_IN =
      new StepRegistry(
          Map.of(
              "remove", RemoveStep.FACTORY, "rename", RenameStep.FACTORY, "set", SetStep.FACTORY));

  private final Map<String, Factory> factories;

  private StepRegistry(Map<String, Factory> factories) {
    this.factories = factories;
  }

  /** A registry of exactly {@code factories}, keyed by op name. */
  public static StepRegistry of(Map<String, Factory> factories) {
    Objects.requireNonNull(factories, "factories");
    return new StepRegistry(Collections.unmodifiableMap(new LinkedHashMap<>(factories)));
  }

  /** The op names. */
  public Set<String> ops() {
    return factories.keySet();
  }

  /** The factory of {@code op}, or {@code null} if there is none. */
  @Nullable Factory factory(String op) {
    return factories.get(op);
  }
}
