package MasterWorker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Custom {@link java.util.HashMap} using a LRU policy.
 *
 * @param <K> key
 * @param <V> value
 * @author Shintaro Katafuchi
 */
final class LruHashMap<K, V> extends LinkedHashMap<K, V> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final int capacity;

    public LruHashMap(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    @SuppressWarnings("rawtypes")
	@Override
    protected boolean removeEldestEntry(Map.Entry entry) {
        return size() > capacity;
    }

}