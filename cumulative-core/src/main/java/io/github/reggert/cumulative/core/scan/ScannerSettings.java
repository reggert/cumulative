package io.github.reggert.cumulative.core.scan;

import org.apache.accumulo.core.security.Authorizations;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;

import static java.util.Objects.requireNonNull;


/**
 * Immutable representation of settings to use for a scan.
 */
public abstract class ScannerSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Duration timeout;
    private final Duration batchTimeout;
    private final String classloaderContext;
    private final Authorizations authorizations;


    public static final class Simple extends ScannerSettings {
        private static final long serialVersionUID = 1L;
        private final Integer batchSize;
        private final Long readAheadThreshold;
        private final boolean isolationEnabled;
        public static final Simple DEFAULT = builder().build();

        private Simple(
            @Nullable final Duration timeout,
            @Nullable final Duration batchTimeout,
            @Nullable final String classloaderContext,
            @Nonnull final Authorizations authorizations,
            @Nullable final Integer batchSize,
            @Nullable final Long readAheadThreshold,
            final boolean isolationEnabled
        ) {
            super(timeout, batchTimeout, classloaderContext, authorizations);
            this.batchSize = batchSize;
            this.readAheadThreshold = readAheadThreshold;
            this.isolationEnabled = isolationEnabled;
        }

        public Optional<Integer> getBatchSize() {
            return Optional.ofNullable(batchSize);
        }

        public Optional<Long> getReadAheadThreshold() {
            return Optional.ofNullable(readAheadThreshold);
        }

        public boolean isIsolationEnabled() {
            return isolationEnabled;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            final Simple that = (Simple) o;
            return this.isolationEnabled == that.isolationEnabled &&
                Objects.equals(this.batchSize, that.batchSize) &&
                Objects.equals(this.readAheadThreshold, that.readAheadThreshold);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), batchSize, readAheadThreshold, isolationEnabled);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ScannerSettings.Simple{");
            sb.append("timeout=").append(getTimeout().orElse(null));
            sb.append(", batchTimeout=").append(getBatchTimeout().orElse(null));
            sb.append(", classloaderContext='").append(getClassloaderContext().orElse(null)).append('\'');
            sb.append(", authorizations=").append(getAuthorizations());
            sb.append(", batchSize=").append(batchSize);
            sb.append(", readAheadThreshold=").append(readAheadThreshold);
            sb.append(", isolationEnabled=").append(isolationEnabled);
            sb.append('}');
            return sb.toString();
        }

        public static final class Builder extends ScannerSettings.Builder {
            private Integer batchSize;
            private Long readAheadThreshold;
            private boolean isolationEnabled;

            private Builder() {
                this.batchSize = null;
                this.readAheadThreshold = null;
                this.isolationEnabled = false;
            }

            private Builder(@Nonnull final ScannerSettings.Simple source) {
                super(source);
                this.batchSize = source.batchSize;
                this.readAheadThreshold = source.readAheadThreshold;
                this.isolationEnabled = source.isolationEnabled;
            }

            public Builder batchSize(final int batchSize) {
                this.batchSize = batchSize;
                return this;
            }

            public Builder clearBatchSize() {
                this.batchSize = null;
                return this;
            }

            public Builder readAheadThreshold(final long readAheadThreshold) {
                this.readAheadThreshold = readAheadThreshold;
                return this;
            }

            public Builder clearReadAheadThreshold() {
                this.readAheadThreshold = null;
                return this;
            }

            public Builder isolationEnabled(final boolean isolationEnabled) {
                this.isolationEnabled = isolationEnabled;
                return this;
            }

            public Simple build() {
                return new ScannerSettings.Simple(
                    timeout,
                    batchTimeout,
                    classloaderContext,
                    authorizations,
                    batchSize,
                    readAheadThreshold,
                    isolationEnabled
                );
            }
        }

        /**
         * Constructs a new builder with default settings.
         *
         * @return a new {@code builder}.
         */
        public static Builder builder() {
            return new Builder();
        }


        /**
         * Copies the settings from this object into a new builder.
         *
         * @return a new {@code Builder}.
         */
        public Builder toBuilder() {
            return new Builder(this);
        }
    }


    public static final class Batch extends ScannerSettings {
        private static final long serialVersionUID = 1L;
        private final int numberOfQueryThreads;
        public static final int DEFAULT_QUERY_THREADS = 2;
        public static final Batch DEFAULT = builder().build();

        private Batch(
            @Nullable final Duration timeout,
            @Nullable final Duration batchTimeout,
            @Nullable final String classloaderContext,
            @Nonnull final Authorizations authorizations,
            final int numberOfQueryThreads
        ) {
            super(timeout, batchTimeout, classloaderContext, authorizations);
            this.numberOfQueryThreads = numberOfQueryThreads;
        }

        public int getNumberOfQueryThreads() {
            return numberOfQueryThreads;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            final Batch batch = (Batch) o;
            return numberOfQueryThreads == batch.numberOfQueryThreads;
        }

        @Override
        public int hashCode() {

            return Objects.hash(super.hashCode(), numberOfQueryThreads);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ScannerSettings.Batch{");
            sb.append("timeout=").append(getTimeout().orElse(null));
            sb.append(", batchTimeout=").append(getBatchTimeout().orElse(null));
            sb.append(", classloaderContext='").append(getClassloaderContext().orElse(null)).append('\'');
            sb.append(", authorizations=").append(getAuthorizations());
            sb.append(", numberOfQueryThreads=").append(numberOfQueryThreads);
            sb.append('}');
            return sb.toString();
        }

        public static final class Builder extends ScannerSettings.Builder {
            private int numberOfQueryThreads;

            private Builder() {
                this.numberOfQueryThreads = DEFAULT_QUERY_THREADS;
            }

            private Builder(@Nonnull final ScannerSettings.Batch source) {
                super(source);
                this.numberOfQueryThreads = source.numberOfQueryThreads;
            }

            public Builder numberOfQueryThreads(final int numberOfQueryThreads) {
                this.numberOfQueryThreads = numberOfQueryThreads;
                return this;
            }

            public Batch build() {
                return new ScannerSettings.Batch(
                    timeout,
                    batchTimeout,
                    classloaderContext,
                    authorizations,
                    numberOfQueryThreads
                );
            }
        }

        /**
         * Constructs a new builder with default settings.
         *
         * @return a new {@code builder}.
         */
        public static Builder builder() {
            return new Builder();
        }


        /**
         * Copies the settings from this object into a new builder.
         *
         * @return a new {@code Builder}.
         */
        public Builder toBuilder() {
            return new Builder(this);
        }
    }


    public static abstract class Builder {
        protected Duration timeout;
        protected Duration batchTimeout;
        protected String classloaderContext;
        protected Authorizations authorizations;

        private Builder() {
            this.timeout = null;
            this.batchTimeout = null;
            this.classloaderContext = null;
            this.authorizations = new Authorizations();
        }

        private Builder(@Nonnull final ScannerSettings source) {
            this.timeout = source.timeout;
            this.batchTimeout = source.batchTimeout;
            this.classloaderContext = source.classloaderContext;
            this.authorizations = source.authorizations;
        }

        public Builder timeout(@Nonnull final Duration timeout) {
            requireNonNull(timeout, "timeout");
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            this.timeout = timeout;
            return this;
        }

        public Builder clearTimeout() {
            this.timeout = null;
            return this;
        }

        public Builder batchTimeout(@Nonnull final Duration batchTimeout) {
            requireNonNull(batchTimeout, "batchTimeout");
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("batchTimeout must be positive");
            }
            this.batchTimeout = batchTimeout;
            return this;
        }

        public Builder clearBatchTimeout() {
            this.batchTimeout = null;
            return this;
        }

        public Builder classloaderContext(@Nonnull final String classloaderContext) {
            this.classloaderContext = requireNonNull(classloaderContext, "classloaderContext");
            return this;
        }

        public Builder clearClassloaderContext() {
            this.classloaderContext = null;
            return this;
        }
    }


    private ScannerSettings(
        @Nullable final Duration timeout,
        @Nullable final Duration batchTimeout,
        @Nullable final String classloaderContext,
        @Nonnull final Authorizations authorizations
    ) {
        this.timeout = timeout;
        this.batchTimeout = batchTimeout;
        this.classloaderContext = classloaderContext;
        this.authorizations = authorizations;
    }


    public Optional<Duration> getTimeout() {
        return Optional.ofNullable(timeout);
    }


    public Optional<Duration> getBatchTimeout() {
        return Optional.ofNullable(batchTimeout);
    }


    public Optional<String> getClassloaderContext() {
        return Optional.ofNullable(classloaderContext);
    }


    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ScannerSettings that = (ScannerSettings) o;
        return Objects.equals(this.timeout, that.timeout) &&
            Objects.equals(this.batchTimeout, that.batchTimeout) &&
            Objects.equals(this.classloaderContext, that.classloaderContext) &&
            Objects.equals(this.authorizations, that.authorizations);
    }


    @Override
    public int hashCode() {
        return Objects.hash(timeout, batchTimeout, classloaderContext, authorizations);
    }

}
