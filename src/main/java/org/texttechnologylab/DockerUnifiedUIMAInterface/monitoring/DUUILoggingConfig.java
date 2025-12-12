package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.DebugLevel;

/**
 * Central configuration for DUUI logging.
 * <p>
 * Loads per-logger and per-package minimum levels from a classpath
 * properties file (defaults to {@code duui-logging.properties}).
 */
public final class DUUILoggingConfig {

    private static final String DEFAULT_RESOURCE = "duui-logging.properties";

    private static final ConcurrentMap<String, DebugLevel> PER_LOGGER_MIN_LEVEL =
            new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, DebugLevel> PER_PACKAGE_MIN_LEVEL =
            new ConcurrentHashMap<>();

    private static volatile DebugLevel DEFAULT_MIN_LEVEL = DebugLevel.TRACE;

    // Formatting / emission flags
    private static volatile boolean INCLUDE_CLASS_NAME = false;
    private static volatile boolean INCLUDE_SENDER = true;
    private static volatile boolean INCLUDE_PAYLOAD = true;
    private static volatile boolean FORMAT_TIMESTAMP = false;

    private DUUILoggingConfig() {
    }

    static {
        loadFromClasspath(DEFAULT_RESOURCE);
    }

    /**
     * Load logging configuration from the given classpath resource.
     * Existing configuration is preserved; new values override previous ones.
     */
    public static void loadFromClasspath(String resourceName) {
        Properties props = new Properties();
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = DUUILoggingConfig.class.getClassLoader();
        }
        try (InputStream in = cl.getResourceAsStream(resourceName)) {
            if (in == null) {
                return;
            }
            props.load(in);
        } catch (IOException e) {
            // ignore config loading errors and keep defaults
            return;
        }

        // Formatting flags
        String includeClassName = props.getProperty("logging.includeClassName");
        if (includeClassName != null) {
            INCLUDE_CLASS_NAME = Boolean.parseBoolean(includeClassName.trim());
        }

        String includeSender = props.getProperty("logging.includeSender");
        if (includeSender != null) {
            INCLUDE_SENDER = Boolean.parseBoolean(includeSender.trim());
        }

        String includePayload = props.getProperty("logging.includePayload");
        if (includePayload != null) {
            INCLUDE_PAYLOAD = Boolean.parseBoolean(includePayload.trim());
        }

        String formatTimestamp = props.getProperty("logging.formatTimestamp");
        if (formatTimestamp != null) {
            FORMAT_TIMESTAMP = Boolean.parseBoolean(formatTimestamp.trim());
        }

        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = ((String) entry.getKey()).trim();
            String value = ((String) entry.getValue()).trim();

            if (!key.startsWith("logger.")) {
                continue;
            }

            String target = key.substring("logger.".length());

            if ("default".equals(target)) {
                DebugLevel level = parseLevel(value);
                if (level != null) {
                    DEFAULT_MIN_LEVEL = level;
                }
                continue;
            }

            if (target.endsWith(".*")) {
                String pkg = target.substring(0, target.length() - 2);
                DebugLevel level = parseLevel(value);
                if (level != null) {
                    PER_PACKAGE_MIN_LEVEL.put(pkg, level);
                }
                continue;
            }

            DebugLevel level = parseLevel(value);
            if (level != null) {
                PER_LOGGER_MIN_LEVEL.put(target, level);
            }
        }
    }

    private static DebugLevel parseLevel(String value) {
        String v = value.trim().toUpperCase();
        try {
            return DebugLevel.valueOf(v);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public static void setDefaultMinLevel(DebugLevel level) {
        DEFAULT_MIN_LEVEL = level;
    }

    public static void setMinLevel(Class<?> clazz, DebugLevel level) {
        PER_LOGGER_MIN_LEVEL.put(clazz.getName(), level);
    }

    public static boolean isIncludeClassName() {
        return INCLUDE_CLASS_NAME;
    }

    public static void setIncludeClassName(boolean includeClassName) {
        INCLUDE_CLASS_NAME = includeClassName;
    }

    public static boolean isIncludeSender() {
        return INCLUDE_SENDER;
    }

    public static void setIncludeSender(boolean includeSender) {
        INCLUDE_SENDER = includeSender;
    }

    public static boolean isIncludePayload() {
        return INCLUDE_PAYLOAD;
    }

    public static void setIncludePayload(boolean includePayload) {
        INCLUDE_PAYLOAD = includePayload;
    }

    public static boolean isFormatTimestamp() {
        return FORMAT_TIMESTAMP;
    }

    public static void setFormatTimestamp(boolean formatTimestamp) {
        FORMAT_TIMESTAMP = formatTimestamp;
    }

    /**
     * Returns the configured minimum level for the given logger name,
     * falling back to package-level configuration and finally the
     * global default.
     */
    public static DebugLevel getMinLevel(String loggerName) {
        DebugLevel level = PER_LOGGER_MIN_LEVEL.get(loggerName);
        if (level != null) {
            return level;
        }

        DebugLevel best = null;
        int bestLen = -1;
        for (Map.Entry<String, DebugLevel> e : PER_PACKAGE_MIN_LEVEL.entrySet()) {
            String pkg = e.getKey();
            if (loggerName.startsWith(pkg) && pkg.length() > bestLen) {
                best = e.getValue();
                bestLen = pkg.length();
            }
        }

        return best != null ? best : DEFAULT_MIN_LEVEL;
    }
}
