package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Duration;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.DUUIContext.DocumentComponentProcessContext;

/**
 * Lightweight logging facade used throughout DUUI.
 * <p>
 * Default methods use the thread-local context from {@link DUUILogContext}
 * and delegate emission to the {@link #emit(DUUIEvent)} method implemented
 * by concrete loggers.
 */
public interface DUUILogger extends DUUIEventEmitter {

        /**
         * @return the logical name of this logger. Implementations are free to
         *         choose an appropriate convention (for example, the fully
         *         qualified class name when using class-scoped loggers).
         */
        default String name() {
            return "";
        }

        /**
         * @return the default {@link DUUIEvent.Sender} associated with this
         *         logger. Class-scoped loggers should derive this from the
         *         owning class.
         */
        default DUUIEvent.Sender sender() {
            return DUUIEvent.Sender.SYSTEM;
        }



        interface MeasurementScope extends AutoCloseable {
            void markError();
            @Override void close();
        }

        MeasurementScope NOOP_MEASUREMENT = new MeasurementScope() {
            @Override public void markError() {}
            @Override public void close() {}
        };
        

        default MeasurementScope measureStep(@Nonnull DUUIStatus step, Duration elapsed, String format, Object... args) {
            Objects.requireNonNull(step);

            updateStatus(step);

            if (elapsed != null) {
                measureStep(getContext(), elapsed, format, args);
                return NOOP_MEASUREMENT;
            } else {
                return measureStep(getContext(), format, args);
            }
        }

        default MeasurementScope measureStep(@Nonnull DUUIStatus step, String format, Object... args) {
            return measureStep(step, null, format, args);
        }

        default MeasurementScope measureStep(DUUIContext ctx, String format, Object... args) {
            return measureStep(ctx, String.format(format, args));
        }

        default MeasurementScope measureStep(DUUIContext ctx, final String message) {
            var composer = DUUIContextTL.COMPOSER.get();

            if (composer == null || composer.getProcess() == null || !composer.getProcess().hasProfiler()) {
                return NOOP_MEASUREMENT;
            }

            String stepLabel = ctx.status().toString();
            String  tmp = "(none)";
            
            if (ctx instanceof DocumentComponentProcessContext dctx) {
                tmp = dctx.component().component().componentName();
            }

            final String componentLabel = tmp;

            DUUIProfiler.Timer timer = DUUIProfiler.timeStep(stepLabel, componentLabel);
            return new MeasurementScope() {
                @Override public void markError() { DUUIProfiler.Timer.markError(stepLabel, componentLabel); }
                @Override public void close() { 
                    timer.close(); 
                    String elapsedString = String.format(" %d ms", timer.elapsed().toMillis());
                    debug(ctx.metric(timer.elapsed()), message + elapsedString);
                }
            };
        }

        default void measureStep(DUUIContext ctx, Duration elapsed, String format, Object... args) {
            String message = String.format(format, args);

            var composer = DUUIContextTL.COMPOSER.get();

            if (composer == null || composer.getProcess() == null || !composer.getProcess().hasProfiler()) {
                return;
            }

            String stepLabel = ctx.status().toString();
            String  tmp = "(none)";
            
            if (ctx instanceof DocumentComponentProcessContext dctx) {
                tmp = dctx.component().component().componentName();
            }

            final String componentLabel = tmp;

            DUUIProfiler.timeStep(stepLabel, componentLabel, elapsed);

            String elapsedString = String.format(" %d ms", elapsed.toMillis());
            debug(ctx.metric(elapsed), message + elapsedString);

        }

        default MeasurementScope measureDocument(DUUIContext ctx, DUUIDocument document) {
            var composer = DUUIContextTL.COMPOSER.get();

            if (composer == null || composer.getProcess() == null || !composer.getProcess().hasProfiler()) {
                return NOOP_MEASUREMENT;
            }

            DUUIProfiler.DocRun docRun = DUUIProfiler.docRun(document);
            return new MeasurementScope() {
                @Override public void markError() { docRun.markError(); }
                @Override public void close() { 
                    docRun.close(); 
                    debug(ctx.metric(docRun.elapsed()), 
                        "%s has been processed after %d ms", document, docRun.elapsed().toMillis());
                }
            };
        }

        /**
         * Scope handle for temporary logging contexts.
         * <p>
         * Extends {@link AutoCloseable} but does not declare any checked
         * exceptions on {@link #close()}, so it can be used in
         * try-with-resources without forcing callers to handle or declare
         * checked exceptions.
         */
        interface ContextScope extends AutoCloseable {
            void updateStatus(DUUIStatus status);

            @Override
            void close();
        }

        default DUUIContext getContext() {
            return DUUILogContext.getContext();
        }

        default void updateStatus(@Nonnull DUUIStatus status) {
            setContext(getContext().updateStatus(status));
        }

        default void setContext(DUUIContext context) {
            DUUILogContext.setContext(context);
        }

        /**
         * Temporarily sets the logging context for the current thread and
         * returns an {@link AutoCloseable} that restores the previous context
         * when closed. Intended for use with try-with-resources:
         *
         * <pre>{@code
         * try (var ignored = logger.withContext(context)) {
         *     logger.info("message");
         * }
         * }</pre>
         */
        default ContextScope withContext(DUUIContext context) {
            
            DUUIContext previous = getContext();
            setContext(context);
            // DUUILogger previousL = this;
            // DUUILogContext.setLogger(this);
            return new ContextScope() {
                @Override
                public void updateStatus(DUUIStatus status) {
                    setContext(getContext().updateStatus(status));
                }

                @Override
                public void close() {
                    setContext(previous);
                    // DUUILogContext.setLogger(previousL);
                }
            };
        } 

        default void log(DUUIContext context, DebugLevel level, String message) {
            emit(new DUUIEvent(sender(), message, level, context, name()));
        }

        default void log(DUUIContext context, DebugLevel level, String format, Object... args) {
            log(context, level, String.format(format, args));
        }

        default void trace(String format, Object... args) {
            trace(getContext(), format, args);
        }

        default void debug(String format, Object... args) {
            debug(getContext(), format, args);
        }

        default void info(String format, Object... args) {
            info(getContext(), format, args);
        }

        default void warn(String format, Object... args) {
            warn(getContext(), format, args);
        }

        default void error(String format, Object... args) {
            error(getContext(), format, args);
        }

        default void critical(String format, Object... args) {
            critical(getContext(), format, args);
        }

        default void none(String format, Object... args) {
            log(getContext(), DebugLevel.NONE, format, args);
        }

        default void trace(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.TRACE, format, args);
        }

        default void debug(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.DEBUG, format, args);
        }

        default void info(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.INFO, format, args);
        }

        default void warn(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.WARN, format, args);
        }

        default void error(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.ERROR, format, args);
        }

        default void critical(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.CRITICAL, format, args);
        }

        default void none(DUUIContext context, String format, Object... args) {
            log(context, DebugLevel.NONE, format, args);
        }

        default void trace(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.TRACE, format, args);
        }

        default void debug(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.DEBUG, format, args);
        }

        default void info(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.INFO, format, args);
        }

        default void warn(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.WARN, format, args);
        }

        default void error(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.ERROR, format, args);
        }

        default void critical(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.CRITICAL, format, args);
        }

        default void none(DUUIStatus status, String format, Object... args) {
            log(getContext().updateStatus(status), DebugLevel.NONE, format, args);
        }

        default void trace(String message) {
            log(getContext(), DebugLevel.TRACE, message);
        }

        default void debug(String message) {
            log(getContext(), DebugLevel.DEBUG, message);
        }

        default void info(String message) {
            log(getContext(), DebugLevel.INFO, message);
        }

        default void warn(String message) {
            log(getContext(), DebugLevel.WARN, message);
        }

        default void error(String message) {
            log(getContext(), DebugLevel.ERROR, message);
        }

        default void critical(String message) {
            log(getContext(), DebugLevel.CRITICAL, message);
        }

        default void none(String message) {
            log(getContext(), DebugLevel.NONE, message);
        }
    }
