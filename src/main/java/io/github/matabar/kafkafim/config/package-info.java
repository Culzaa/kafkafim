/**
 * Configuration model, loading, validation and hot-reload diffing, bound through Spring Boot.
 * Record components that the binder leaves null before the compact constructors apply defaults are
 * not yet annotated {@code @Nullable}.
 */
@NullMarked
package io.github.matabar.kafkafim.config;

import org.jspecify.annotations.NullMarked;
