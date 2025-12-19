package org.example;

import java.util.*;

class Main {
    public static void main(String[] args) {
        List<String> servers = Arrays.asList("S0", "S1", "S2", "S3", "S4", "S5");
        ConsistentHashing ch = new ConsistentHashing(servers, 3);

        // Step 2: Assign requests (keys) to servers
        System.out.println("UserA is assigned to: " + ch.getServer("User A"));
        System.out.println("UserB is assigned to: " + ch.getServer("UserB"));

        // Step 3: Add a new server dynamically
        ch.addServer("S6");
        System.out.println("UserA is now assigned to: " + ch.getServer("UserA"));

        // Step 4: Remove a server dynamically
        ch.removeServer("S2");
        System.out.println("UserB is now assigned to: " + ch.getServer("UserB"));

    }
}
