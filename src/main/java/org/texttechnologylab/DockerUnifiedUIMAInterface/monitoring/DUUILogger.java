package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;

/**
 * Lightweight logging facade used throughout DUUI.
 * <p>
 * Default methods use the thread-local context from {@link DUUILogContext}
 * and delegate emission to the {@link #emit(DUUIEvent)} method implemented
 * by concrete loggers.
 */
public interface DUUILogger extends DUUIEventEmitter {

        /**
         * Scope handle for temporary logging contexts.
         * <p>
         * Extends {@link AutoCloseable} but does not declare any checked
         * exceptions on {@link #close()}, so it can be used in
         * try-with-resources without forcing callers to handle or declare
         * checked exceptions.
         */
        interface ContextScope extends AutoCloseable {
            @Override
            void close();
        }

        default DUUIEvent.Context getContext() {
            return DUUILogContext.getContext();
        }

        default void setContext(DUUIEvent.Context context) {
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
        default ContextScope withContext(DUUIEvent.Context context) {
            DUUIEvent.Context previous = getContext();
            setContext(context);
            return () -> setContext(previous);
        }

        default void log(DUUIEvent.Context context, DebugLevel level, String message) {
            emit(new DUUIEvent(context.sender(), message, level, context));
        }

        default void log(DUUIEvent.Context context, DebugLevel level, String format, Object... args) {
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

        default void trace(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.TRACE, format, args);
        }

        default void debug(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.DEBUG, format, args);
        }

        default void info(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.INFO, format, args);
        }

        default void warn(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.WARN, format, args);
        }

        default void error(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.ERROR, format, args);
        }

        default void critical(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.CRITICAL, format, args);
        }

        default void none(DUUIEvent.Context context, String format, Object... args) {
            log(context, DebugLevel.NONE, format, args);
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


        // String status; 
        
        // String runKey;
        
        // String documentId;
        
        // String driver;
        // String componentName; 
        // String componentId;
        // String componentInstanceId;

        // String message;

        // String logs;

        // DUUIComposer composer;
        
        // String prefix;
        // Sender sender; 
    }
