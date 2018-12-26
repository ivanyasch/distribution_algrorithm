import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public class ReplicatedStockServer extends ReceiverAdapter {
    private final Map<String, Double> stocks = new HashMap<>();
    private JChannel channel;
    private LockService lockService;
    private RpcDispatcher rpcDispatcher;

    public void _setStock(String name, double value) {
        synchronized (stocks) {
            stocks.put(name, value);
            System.out.printf("set %s to %s\n", name, value);
        }
    }

    public void _removeStock(String name) {
        synchronized (stocks) {
            stocks.remove(name);
            System.out.printf("removed %s\n", name);
        }
    }

    private void start(String props, String name) throws Exception {
        channel = new JChannel(props).name(name);
        rpcDispatcher = new RpcDispatcher(channel, this).setMembershipListener(this);
        lockService = new LockService(channel);
        rpcDispatcher.setStateListener(this);
        channel.connect("stocks");
        rpcDispatcher.start();
        channel.getState(null, 30000);
        while (true) {
            int c = Util.keyPress("[1] Show stocks [2] Get quote [3] Set quote [4] Remove quote [5] CompareAndSwap [x] Exit");
            try {
                switch (c) {
                    case '1':
                        showStocks();
                        break;
                    case '2':
                        getStock();
                        break;
                    case '3':
                        setStock();
                        break;
                    case '4':
                        removeStock();
                        break;
                    case '5':
                        compareAndSwap();
                        break;
                    case 'x':
                        channel.close();
                        return;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Set a stock if it's value equal to ref
     */
    public void _compareAndSwap(String name, double reference, double newValue) {
        synchronized (stocks) {
            Double val = stocks.get(name);
            if (val == reference) {
                stocks.put(name, newValue);
                System.out.printf("CAS -- set %s to %s\n", name, newValue);
            } else {
                System.out.printf("CAS -- failed in setting %s to %s\n", name, newValue);
            }
        }
    }

    /**
     * Compare value and swap it if value by key == reference value
     */
    private void compareAndSwap() throws Exception {
        String key = readString("key");
        String referenceValue = readString("referenceValue");
        Double referenceValueDouble = Double.parseDouble(referenceValue);
        String newValue = readString("newValue");
        Double newValueDouble = Double.parseDouble(newValue);
        Lock lock = lockService.getLock("lock " + key);
        lock.lock();
        try {
            RspList<Void> rspList = rpcDispatcher.callRemoteMethods(null, "_compareAndSwap",
                    new Object[]{key, referenceValueDouble, newValueDouble},
                    new Class[]{String.class, double.class, double.class}, RequestOptions.SYNC());
            System.out.println("rspList:\n" + rspList);
        } finally {
            lock.unlock();
        }
    }

    public void viewAccepted(View newView) {
        System.out.println("View: " + newView);
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        DataOutput outputStream = new DataOutputStream(output);
        synchronized (stocks) {
            System.out.println("returning " + stocks.size() + " stocks");
            Util.objectToStream(stocks, outputStream);
        }
    }

    @Override
    public void setState(InputStream input) throws Exception {
        DataInput inputStream = new DataInputStream(input);
        Map<String, Double> new_state = Util.objectFromStream(inputStream);
        System.out.println("received state: " + new_state.size() + " stocks");
        synchronized (stocks) {
            stocks.clear();
            stocks.putAll(new_state);
        }
    }

    private void getStock() throws IOException {
        String ticker = readString("Symbol");
        synchronized (stocks) {
            Double val = stocks.get(ticker);
            System.out.println(ticker + " is " + val);
        }
    }

    private void setStock() throws Exception {
        String ticker, val;
        ticker = readString("Symbol");
        val = readString("Value");
        RspList<Void> rspList = rpcDispatcher.callRemoteMethods(null, "_setStock", new Object[]{ticker, Double.parseDouble(val)},
                new Class[]{String.class, double.class}, RequestOptions.SYNC());
        System.out.println("rspList:\n" + rspList);
    }


    private void removeStock() throws Exception {
        String ticker = readString("Symbol");
        RspList<Void> rspList = rpcDispatcher.callRemoteMethods(null, "_removeStock", new Object[]{ticker},
                new Class[]{String.class}, RequestOptions.SYNC());
        System.out.println("rspList:\n" + rspList);
    }

    private void showStocks() {
        System.out.println("Stocks:");
        synchronized (stocks) {
            for (Map.Entry<String, Double> entry : stocks.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    private static String readString(String s) throws IOException {
        int character;
        boolean looping = true;
        StringBuilder stringBuilder = new StringBuilder();
        System.out.print(s + ": ");
        System.out.flush();
        System.in.skip(System.in.available());

        while (looping) {
            character = System.in.read();
            switch (character) {
                case -1:
                case '\n':
                case 13:
                    looping = false;
                    break;
                default:
                    stringBuilder.append((char) character);
                    break;
            }
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        String props = "config.xml";
        String name = "Y";

        try {
            new ReplicatedStockServer().start(props, name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
