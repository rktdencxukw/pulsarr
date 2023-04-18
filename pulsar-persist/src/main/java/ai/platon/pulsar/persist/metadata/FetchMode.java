package ai.platon.pulsar.persist.metadata;

import org.jetbrains.annotations.NotNull;

/**
 * @author vincent
 */
public enum FetchMode {
    UNKNOWN,
    /**
     * Simple native fetcher, no script renderer/cookie supported
     * */
    NATIVE,
    /**
     * Fetch every page using a real browser
     * */
    BROWSER;

    /**
     * <p>fromString.</p>
     *
     * @param s a {@link java.lang.String} object.
     * @return a {@link ai.platon.pulsar.persist.metadata.FetchMode} object.
     */
    @NotNull
    public static FetchMode fromString(String s) {
        if (s == null || s.isEmpty()) {
            return UNKNOWN;
        }

        try {
            return FetchMode.valueOf(s.toUpperCase());
        } catch (Throwable e) {
            throw new IllegalArgumentException("Invalid fetch mode: " + s);
//            return UNKNOWN;
        }
    }
}
