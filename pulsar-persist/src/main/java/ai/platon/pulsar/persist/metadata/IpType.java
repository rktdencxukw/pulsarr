package ai.platon.pulsar.persist.metadata;

import org.jetbrains.annotations.NotNull;

/**
 * @author vincent
 */
public enum IpType {
    UNKNOWN,
    /**
     * Simple native fetcher, no script renderer/cookie supported
     * */
    RESIDENCE,
    /**
     * Fetch every page using a real browser
     * */
    SERVER;

    /**
     * <p>fromString.</p>
     *
     * @param s a {@link String} object.
     * @return a {@link IpType} object.
     */
    @NotNull
    public static IpType fromString(String s) {
        if (s == null || s.isEmpty()) {
            return UNKNOWN;
        }

        try {
            return IpType.valueOf(s.toUpperCase());
        } catch (Throwable e) {
            throw new IllegalArgumentException("Invalid ip type: " + s);
//            return UNKNOWN;
        }
    }
}
