package org.example;

import java.util.LinkedList;
import java.util.List;

public class ServerData {
    public final List<Entry> hot = new LinkedList<>();
    public final List<Entry> warm = new LinkedList<>();
    public final List<Entry> cold = new LinkedList<>();
}