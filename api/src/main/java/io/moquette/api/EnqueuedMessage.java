package io.moquette.api;

public abstract class EnqueuedMessage {

    /**
     * Releases any held resources. Must be called when the EnqueuedMessage is no
     * longer needed.
     */
    public void release() {
    }

    /**
     * Retains any held resources. Must be called when the EnqueuedMessage is added
     * to a store.
     */
    public void retain() {
    }
}
