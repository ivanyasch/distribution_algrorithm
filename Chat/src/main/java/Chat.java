import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Chat extends ReceiverAdapter {
    private JChannel channel;

    @Override
    public void viewAccepted(View newView) {
        System.out.println("View: " + newView);
    }

    public void receive(Message message) {
        String objectMessage = message.getObject();
        System.out.printf("name %s: %s\n", message.getSrc(), objectMessage);
    }

    private void start(String properties, String name) throws Exception {
        channel = new JChannel(properties).name(name).receiver(this);
        channel.connect("Chat");
        JmxConfigurator.registerChannel(channel, Util.getMBeanServer(),
                "chat-channel", channel.getClusterName(), true);
        eventLoop();
        channel.close();
    }

    private void eventLoop() {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                System.out.print(">> ");
                System.out.flush();
                String line = bufferedReader.readLine().toLowerCase();
                if (line.startsWith("quit") || line.startsWith("exit"))
                    break;
                Message message = new Message(null, line);
                channel.send(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            new Chat().start("config.xml", "T");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}