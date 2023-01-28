package io.moquette.broker.unsafequeues;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VirtualPointerTest {

    @Test
    public void testCompare() {
        final VirtualPointer lower = new VirtualPointer(100L);
        final VirtualPointer higher = new VirtualPointer(200L);

        assertEquals(1, higher.compareTo(lower), higher.logicalOffset + " must be greater than " + lower.logicalOffset);
        assertEquals(-1, lower.compareTo(higher), lower.logicalOffset + " must be lower than " + higher.logicalOffset);
        assertEquals(0, lower.compareTo(lower), "identity must be equal");
    }
}
