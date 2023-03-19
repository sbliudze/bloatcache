package edu.kit.kastel.formal.bloatcache;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ServerData {
    public final Set<Entry.Key> keys = new HashSet<>();
    public final List<Entry> hot = new LinkedList<>();
    public final List<Entry> warm = new LinkedList<>();
    public final List<Entry> cold = new LinkedList<>();

    public boolean delete(Entry.Key key) {
        if (keys.contains(key)) {
            synchronized (hot) {
                boolean b = hot.removeIf(it -> it.key.equals(key));
                if (b) return true;
            }
            synchronized (warm) {
                boolean b = warm.removeIf(it -> it.key.equals(key));
                if (b) return true;
            }
            synchronized (cold) {
                boolean b = cold.removeIf(it -> it.key.equals(key));
                if (b) return true;
            }
        }
        return false;
    }

    public Entry get(Entry.Key key) {
        if (keys.contains(key)) {
            synchronized (hot) {
                var e = hot.stream().filter(it -> it.key.equals(key)).findFirst();
                if (e.isPresent()) return e.get();
            }
            synchronized (warm) {
                var e = warm.stream().filter(it -> it.key.equals(key)).findFirst();
                if (e.isPresent()) return e.get();
            }
            synchronized (cold) {
                var e = cold.stream().filter(it -> it.key.equals(key)).findFirst();
                if (e.isPresent()) return e.get();
            }
        }
        return null;
    }

    public boolean insert(Entry entry) {
        if (keys.contains(entry.key)) {
            delete(entry.key);
        }
        keys.add(entry.key);
        warm.add(entry);
        return true;
    }
}