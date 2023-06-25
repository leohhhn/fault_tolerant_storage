package com.leon;

import java.io.*;

public class SnapshotService {
    private String snapshotFilePath = "";
    private Node node;

    public SnapshotService(String snapshotFilePath, Node node) {
        this.snapshotFilePath = snapshotFilePath;
        this.node = node;
    }

    public void makeSnapshot(int lastLogIndex) {
        Snapshot s = new Snapshot(node.getStorageMap(), lastLogIndex);

        try {
            FileOutputStream fos = new FileOutputStream(snapshotFilePath);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(s);
            oos.close();
            fos.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public Snapshot readSnapshot() {
        Snapshot s = null;
        try {
            FileInputStream fis = new FileInputStream(snapshotFilePath);
            ObjectInputStream ois = new ObjectInputStream(fis);
            s = (Snapshot) ois.readObject();
            ois.close();
            fis.close();
        }catch (FileNotFoundException e) {
                System.out.println("No snapshot file found. Continuing normal operation");
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Class not found");
            c.printStackTrace();
            return null;
        }

        return s;
    }

}
