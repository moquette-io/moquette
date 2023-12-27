package io.moquette.persistence;

import java.util.Objects;

final class Couple<K, L> {
    final K v1;
    final L v2;

    public Couple(K v1, L v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public static <K, L> Couple<K, L> of(K v1, L v2) {
        return new Couple<>(v1, v2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Couple<?, ?> couple = (Couple<?, ?>) o;
        return Objects.equals(v1, couple.v1) && Objects.equals(v2, couple.v2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v1, v2);
    }
}
