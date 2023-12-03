package io.moquette.broker.subscriptions;

import java.util.Objects;

/**
 * Shared subscription's name.
 */
// It's public because used by PostOffice
public final class ShareName {
    private final String shareName;

    public ShareName(String shareName) {
        this.shareName = shareName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (o instanceof String) {
            return Objects.equals(shareName, (String) o);
        }
        if (getClass() != o.getClass()) return false;
        ShareName shareName1 = (ShareName) o;
        return Objects.equals(shareName, shareName1.shareName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shareName);
    }

    @Override
    public String toString() {
        return "ShareName{" +
            "shareName='" + shareName + '\'' +
            '}';
    }
}
