/**
 * Configuration model, loading, validation and hot-reload diffing, bound through Spring Boot.
 * Record components that stay optional after construction are {@code @Nullable}. Components whose
 * compact constructor applies a default are not: the binder may pass null in, but the record never
 * holds or returns it, and a record component annotation would also make its accessor nullable.
 */
@NullMarked
package io.github.culzaa.kafkafim.config;

import org.jspecify.annotations.NullMarked;
