package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SimpleDynamoProvider extends ContentProvider {

    public String succ = null;
    public String pre = null;
    final String[] ports = new String[]{"11108", "11112", "11116", "11120", "11124"};
    String first_avd = "11108";
    String succ1 = "";
    String destport = "";
    String succ2 = "";
    String pre1 = "";
    String pre2 = "";
    String slf;
    HashMap<String, Boolean> flag = new HashMap<String, Boolean>();
    HashMap<String, Boolean> isdead = new HashMap<String, Boolean>();
    public static boolean isRestart = false;
    String myPort = "";
    int scount = 0;
    static String ring[] = new String[]{"11124", "11112", "11108", "11116", "11120"};
    static ArrayList<String> a;
    static final int SERVER_PORT = 10000;
    String TAG = "Dynamo";
    ConcurrentHashMap<String, String> result = new ConcurrentHashMap<String, String>();
    static String sfirst_en, first_en;
    public static int count = 0;
    public static ConcurrentHashMap<String, String> hashmatch = new ConcurrentHashMap<String, String>();
    Uri mUri = buildUri("content",
            "edu.buffalo.cse.cse486586.simpledynamo.provider");
    public boolean ch = false;
    HashMap<String, Cursor> cr = new HashMap<String, Cursor>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Context c = getContext();
        DatabaseOps dbo = new DatabaseOps(c);
        dbo.delete(dbo);
        Cursor t = dbo.displaydata(dbo);
        for (int i = 0; i < ring.length; i++) {
            Message m = new Message();
            m.messageType = "DELETE";
            m.mport = myPort;
            m.destport = ring[i];
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    //---------INSERT-----------------------------------
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Context c = getContext();

        DatabaseOps dob = new DatabaseOps(c);
        try {
            String key = values.getAsString(DataTable.TableInfo.key);
            String hashedkey = genHash(key);

            if (hashedkey.compareTo(genHash("5562")) >= 0 && hashedkey.compareTo(genHash("5556")) <= 0) {
                destport = "11112";
            } else if (hashedkey.compareTo(genHash("5556")) >= 0 && hashedkey.compareTo(genHash("5554")) <= 0) {
                destport = "11108";
            } else if (hashedkey.compareTo(genHash("5554")) >= 0 && hashedkey.compareTo(genHash("5558")) <= 0) {
                destport = "11116";
            } else if (hashedkey.compareTo(genHash("5558")) >= 0 && hashedkey.compareTo(genHash("5560")) <= 0) {
                destport = "11120";
            } else {
                destport = "11124";
            }

            if (destport.equalsIgnoreCase(myPort)) {
                dob.insertinto(dob, values);
                Message m = new Message();
                m.messageType = "REPLICATE1";
                m.mport = myPort;
                m.key = values.get(DataTable.TableInfo.key).toString();
                m.destport = succ1;
                m.value = values.get(DataTable.TableInfo.value).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

                m = new Message();
                m.messageType = "REPLICATE2";
                m.mport = myPort;
                m.key = values.get(DataTable.TableInfo.key).toString();
                m.destport = succ2;
                m.value = values.get(DataTable.TableInfo.value).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
            } else {

                String s1 = "", s2 = "";
                switch (destport) {
                    case "11108":
                        s1 = "11116";
                        s2 = "11120";
                        break;
                    case "11112":
                        s1 = "11108";
                        s2 = "11116";
                        break;
                    case "11116":
                        s1 = "11120";
                        s2 = "11124";
                        break;
                    case "11120":
                        s1 = "11124";
                        s2 = "11112";
                        break;
                    case "11124":
                        s1 = "11112";
                        s2 = "11108";
                        break;
                }

                Message m1 = new Message();
                m1.messageType = "INSERT";
                m1.key = values.get(DataTable.TableInfo.key).toString();
                m1.destport = destport;
                m1.mport = myPort;
                m1.value = values.get(DataTable.TableInfo.value).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1);

                Message m = new Message();
                m.messageType = "REPLICATE1";
                m.mport = myPort;
                m.key = values.get(DataTable.TableInfo.key).toString();
                m.destport = s1;
                m.value = values.get(DataTable.TableInfo.value).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

                m = new Message();
                m.messageType = "REPLICATE2";
                m.mport = myPort;
                m.key = values.get(DataTable.TableInfo.key).toString();
                m.destport = s2;
                m.value = values.get(DataTable.TableInfo.value).toString();
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return uri;

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Context c = getContext();
        DatabaseOps d = new DatabaseOps(c);
        d.delete(d);
        try {
            a = new ArrayList<String>();
            for (String p : ports) {
                hashmatch.put(genHash(String.valueOf(Integer.parseInt(p) / 2)), p);
                a.add(genHash(String.valueOf(Integer.parseInt(p) / 2)));
            }
            Collections.sort(a);

            for (String k : hashmatch.keySet()) {
                for (String aa : a) {
                    switch (myPort) {
                        case "11108":
                            succ1 = "11116";
                            succ2 = "11120";
                            pre1 = "11112";
                            pre2 = "11124";
                            break;
                        case "11112":
                            succ1 = "11108";
                            succ2 = "11116";
                            pre1 = "11124";
                            pre2 = "11120";
                            break;
                        case "11116":
                            succ1 = "11120";
                            succ2 = "11124";
                            pre1 = "11108";
                            pre2 = "11112";
                            break;
                        case "11120":
                            succ1 = "11124";
                            succ2 = "11112";
                            pre1 = "11116";
                            pre2 = "11108";
                            break;
                        case "11124":
                            succ1 = "11112";
                            succ2 = "11108";
                            pre1 = "11120";
                            pre2 = "11116";
                            break;
                    }
                }
            }

            BufferedReader reader = null;

            try {
                File file = new File(c.getFilesDir().getAbsolutePath());
                reader = new BufferedReader(new FileReader(file + "/" + "RESTART1"));
                String value = reader.readLine();
                reader.close();
                Log.v("CP- onCreate- Check for Restart", "file read successful (is restart)");
                isRestart = true;
            } catch (Exception e) {
                Log.e("CP- oncreate- Check for Restart",
                        "file read failed, first run (not restart)");
                try {
                    File file = new File(c.getFilesDir().getAbsolutePath(), "RESTART1");
                    FileWriter fileWrite = new FileWriter(file);
                    fileWrite.write("RESTART");
                    fileWrite.close();
                    isRestart = false;
                } catch (Exception e1) {
                    Log.e("CP- onCreate-restart-filewrite", "file write failed");
                    e.printStackTrace();
                }
            }
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if (isRestart) {
                ClientTaskQ cq = new ClientTaskQ();

                Message m = new Message();
                m.messageType = "RECOVERSELF";
                m.mport = myPort;
                m.destport = succ1;
                m.count = "1";
                m.key = succ1;
                flag.put(succ1, true);
                cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
                getReplica1(serverSocket);
                getReplica2(serverSocket);
                d.display(d);

            }
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "In ONCREATE! Can't create a ServerSocket");
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public void getReplica1(ServerSocket serverSocket) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        int sendCount = 1;
        ClientTaskQ cq1 = new ClientTaskQ();
        Message m = new Message();
        m.messageType = "RECOVERSELF";
        m.destport = pre1;
        m.mport = myPort;
        m.key = pre1;
        flag.put(pre1, true);
        m.count = "1";
        cq1.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
        boolean arg1 = cq1.get(10, TimeUnit.SECONDS);
    }

    public void getReplica2(ServerSocket serverSocket) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        int sendCount = 1;
        ClientTaskQ cq1 = new ClientTaskQ();
        Message m = new Message();
        m.messageType = "RECOVERSELF";
        m.destport = pre2;
        m.mport = myPort;
        m.count = "1";
        m.key = pre2;
        flag.put(pre2, true);
        cq1.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
        boolean arg1 = cq1.get(10, TimeUnit.SECONDS);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        Context cc = getContext();

        DatabaseOps dbo = new DatabaseOps(cc);
        if (selection.equalsIgnoreCase("\"@\"")) {
            Cursor c = null;
            c = dbo.displaydata(dbo);
            cr.put(selection, c);
            dbo.display(dbo);
        } else if (selection.equalsIgnoreCase("\"*\"")) {
            ConcurrentHashMap<String, String> h = new ConcurrentHashMap<String, String>();
            ++scount;
            Cursor cursor;
            cursor = dbo.displaydata(dbo);
            dbo.display(dbo);
            cursor.moveToFirst();
            while (cursor.isAfterLast() == false) {
                String key = cursor.getString(0);
                String value = cursor.getString(1);
                result.put(key, value);
                cursor.moveToNext();

            }
            if (scount == 1) {
                sfirst_en = myPort;
            }
            try {
                ClientTaskQ cq = new ClientTaskQ();
                int sendCount = scount;
                isdead.put(selection, false);
                Message m = new Message();
                m.messageType = "QUERYSTAR";
                m.result = result;
                m.key = selection;
                m.count = String.valueOf(sendCount);
                m.mport = sfirst_en;
                m.destport = succ1;
                cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
                flag.put(selection, true);
                boolean arg = cq.get(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }

            if (isdead.get(selection)) {
                try {
                    ClientTaskQ cq = new ClientTaskQ();
                    Message m = new Message();
                    m.messageType = "QUERYSTAR";
                    m.result = result;
                    m.key = selection;
                    m.count = String.valueOf(scount);
                    m.mport = sfirst_en;
                    m.destport = succ2;
                    cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
                    flag.put(selection, true);
                    boolean arg = cq.get(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }

            }

        } else {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // cursor = dbo.retrievefrom(dbo, selection);
            try {
                String destport = "";
                String hashselect = genHash(selection);
                if (hashselect.compareTo(genHash("5562")) >= 0 && hashselect.compareTo(genHash("5556")) <= 0) {
                    destport = "11112";
                } else if (hashselect.compareTo(genHash("5556")) >= 0 && hashselect.compareTo(genHash("5554")) <= 0) {
                    destport = "11108";
                } else if (hashselect.compareTo(genHash("5554")) >= 0 && hashselect.compareTo(genHash("5558")) <= 0) {
                    destport = "11116";
                } else if (hashselect.compareTo(genHash("5558")) >= 0 && hashselect.compareTo(genHash("5560")) <= 0) {
                    destport = "11120";
                } else {
                    destport = "11124";
                }
                String s1 = "", s2 = "";

                switch (destport) {
                    case "11108":
                        s1 = "11116";
                        s2 = "11120";

                        break;
                    case "11112":
                        s1 = "11108";
                        s2 = "11116";

                        break;
                    case "11116":
                        s1 = "11120";
                        s2 = "11124";
                        break;
                    case "11120":
                        s1 = "11124";
                        s2 = "11112";
                        break;
                    case "11124":
                        s1 = "11112";
                        s2 = "11108";
                        break;
                }

                Cursor c = null;
                c = dbo.retrievefrom(dbo, selection);

                if (c.getCount() > 0) {
                    cr.put(selection, c);

                } else {
                    try {

                        ClientTaskQ cq = new ClientTaskQ();
                        String sendCount = "1";
                        isdead.put(selection, false);
                        Message m1 = new Message();
                        m1.messageType = "QUERYPASS";
                        m1.destport = destport;
                        m1.key = selection;
                        m1.mport = myPort;
                        m1.count = sendCount;
                        cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1);
                        flag.put(selection, true);
                        boolean arg = cq.get(10, TimeUnit.SECONDS);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                        isdead.put(selection, true);
                    } catch (Exception e) {
                        isdead.put(selection, true);
                    }

                    if (isdead.get(selection)) {

                        ClientTaskQ cq = new ClientTaskQ();
                        String sendCount = "1";
                        Message m1 = new Message();
                        m1.messageType = "QUERYPASS";
                        m1.destport = s1;
                        m1.key = selection;
                        m1.mport = myPort;
                        m1.count = sendCount;
                        cq.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m1);
                        flag.put(selection, true);
                        boolean arg = cq.get(10, TimeUnit.SECONDS);

                    }

                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }

        }
        Cursor cur = cr.get(selection);
        dbo.close();
        if (cur != null) {

            return cur;
        }

        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

//--------------------------SERVER CODE------------------------------------
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                Socket s = serverSocket.accept();

                Context c = getContext();
                DatabaseOps dbo = new DatabaseOps(c);
                ObjectInputStream input = new ObjectInputStream(s.getInputStream());
                Message m1 = (Message) input.readObject();
                String messageType = m1.messageType;
                input.close();

                if (messageType.equalsIgnoreCase("REPLICATE1")) {
                    ContentValues value = new ContentValues();
                    value.put("key", m1.key);
                    value.put("value", m1.value);
                    dbo.insertinto(dbo, value);

                } else if (messageType.equalsIgnoreCase("INSERT")) {
                    ContentValues value = new ContentValues();
                    value.put("key", m1.key);
                    value.put("value", m1.value);
                    dbo.insertinto(dbo, value);
                } else if (messageType.equalsIgnoreCase("REPLICATE2")) {
                    ContentValues value = new ContentValues();
                    value.put("key", m1.key);
                    value.put("value", m1.value);
                    dbo.insertinto(dbo, value);
                } else if (messageType.equalsIgnoreCase("DELETE")) {
                    dbo.delete(dbo);
                } else if (messageType.equalsIgnoreCase("REPLICATEDONE")) {
                } else if (messageType.equalsIgnoreCase("QUERYPASS")) {
                    ConcurrentHashMap<String, String> h = new ConcurrentHashMap<String, String>();
                    Cursor cc = null;
                    cc = dbo.retrievefrom(dbo, m1.key);
                    if (cc != null) {
                        cc.moveToFirst();
                        String res = myPort + "|";
                        while (cc.isAfterLast() == false) {
                            String key = cc.getString(0);
                            String value = cc.getString(1);
                            h.put(key, value);
                            cc.moveToNext();

                        }
                        Message m = new Message();
                        m.messageType = "QUERYRESPONSE";
                        m.result = h;
                        m.key = m1.key;
                        m.destport = m1.mport;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
                    }
                } else if (messageType.equalsIgnoreCase("QUERYRESPONSE")) {
                    System.out.println(m1.key + "This is query response^^^^^^^^^  " + m1.result.toString());
                    MatrixCursor max = new MatrixCursor(new String[]{"key", "value"});
                    ConcurrentHashMap<String, String> hq = new ConcurrentHashMap<String, String>();
                    hq = m1.result;
                    for (String str : hq.keySet()) {

                        MatrixCursor.RowBuilder bg = max.newRow();
                        bg.add("key", str);
                        bg.add("value", hq.get(str));

                    }
                    max.moveToFirst();
                    cr.put(m1.key, max);
                    flag.put(m1.key, false);
                } else if (messageType.equalsIgnoreCase("QUERYSTAR")) {

                    String val = m1.mport;
                    if (val.equalsIgnoreCase(myPort)) {
                        ConcurrentHashMap<String, String> h = m1.result;
                        MatrixCursor max = new MatrixCursor(new String[]{"key", "value"});
                        for (String str : h.keySet()) {

                            MatrixCursor.RowBuilder bg = max.newRow();
                            bg.add("key", str);
                            bg.add("value", h.get(str));
                            //  slf=str.split(":")[1];
                            Log.v("key", str);
                            Log.v("value", h.get(str));

                        }
                        max.moveToFirst();
                        cr.put(m1.key, max);
                        flag.put(m1.key, false);

                        //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QUERYRESPONSE", message, add, mport,String.valueOf(c));
                    } else {

                        result = m1.result;
                        sfirst_en = val;
                        for (String j : m1.result.keySet()) {
                            result.put(j, m1.result.get(j));
                        }
                        Cursor cv = null;
                        scount = Integer.parseInt(m1.count);
                        cv = query(mUri, null, "\"*\"", null, null);

                    }

                    scount = 0;

                } else if (messageType.equalsIgnoreCase("RECOVERSELF")) {
                    Cursor ccr;
                    ConcurrentHashMap<String, String> h = new ConcurrentHashMap<String, String>();
                    ccr = dbo.displaydata(dbo);
                    if (ccr != null) {
                        ccr.moveToFirst();
                        String res = myPort + "|";
                        while (ccr.isAfterLast() == false) {
                            String key = ccr.getString(0);
                            String value = ccr.getString(1);
                            h.put(key, value);
                            ccr.moveToNext();

                        }
                        Message m = new Message();
                        m.messageType = "RECOVERRESPONSE";
                        m.result = h;
                        m.key = m1.key;
                        m.destport = m1.mport;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
                    }
                } else if (messageType.equalsIgnoreCase("RECOVERRESPONSE")) {
                    String cc = m1.count;
                    ConcurrentHashMap<String, String> h = new ConcurrentHashMap<String, String>();
                    h = m1.result;
                    for (String str : h.keySet()) {

                        String destport = "";
                        String hashselect = genHash(str);
                        if (hashselect.compareTo(genHash("5562")) >= 0 && hashselect.compareTo(genHash("5556")) <= 0) {
                            destport = "11112";
                        } else if (hashselect.compareTo(genHash("5556")) >= 0 && hashselect.compareTo(genHash("5554")) <= 0) {
                            destport = "11108";
                        } else if (hashselect.compareTo(genHash("5554")) >= 0 && hashselect.compareTo(genHash("5558")) <= 0) {
                            destport = "11116";
                        } else if (hashselect.compareTo(genHash("5558")) >= 0 && hashselect.compareTo(genHash("5560")) <= 0) {
                            destport = "11120";
                        } else {
                            destport = "11124";
                        }

                        String s1 = "", s2 = "";

                        switch (destport) {
                            case "11108":
                                s1 = "11116";
                                s2 = "11120";

                                break;
                            case "11112":
                                s1 = "11108";
                                s2 = "11116";

                                break;
                            case "11116":
                                s1 = "11120";
                                s2 = "11124";
                                break;
                            case "11120":
                                s1 = "11124";
                                s2 = "11112";
                                break;
                            case "11124":
                                s1 = "11112";
                                s2 = "11108";
                                break;
                        }

                        if (myPort.equalsIgnoreCase(destport) || myPort.equalsIgnoreCase(s1) || myPort.equalsIgnoreCase(s2)) {

                            ContentValues ccv = new ContentValues();
                            ccv.put("key", str);
                            ccv.put("value", h.get(str));
                            dbo.insertinto(dbo, ccv);
                            //   }
                        }

                    }
                    flag.put(m1.key, false);
                } else if (messageType.equalsIgnoreCase("CHECKALIVE")) {

                    Message m = new Message();
                    m.messageType = "ALIVE";
                    m.destport = m1.mport;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);

                }

                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (Exception e) {
                System.out.println("Exception is:" + e);
                e.printStackTrace();
            }
            return null;
        }
    }

    //----------------------------------CLIENT CODE----------------------------
    private class ClientTask extends AsyncTask<Message, Void, Void> {

        @Override
        protected Void doInBackground(Message... msgs) {
            try {

                Message m = msgs[0];
                String messageType = m.messageType;
                if (messageType.equalsIgnoreCase("REPLICATE1")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(m);
                    out.flush();
                    out.close();
                    socket.close();

                } else if (messageType.equalsIgnoreCase("INSERT")) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(m);
                    out.flush();
                    out.close();
                    socket.close();

                } else if (messageType.equalsIgnoreCase("REPLICATE2")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(m);
                    out.flush();
                    out.close();
                    socket.close();

                } else if (messageType.equalsIgnoreCase("REPLICATEDONE")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(m);
                    out.flush();
                    out.close();
                    socket.close();

                } else if (messageType.equalsIgnoreCase("QUERYRESPONSE")) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    Message msgToSend = msgs[0];
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgToSend);
                    out.flush();
                    out.close();
                    socket.close();
                } else if (messageType.equalsIgnoreCase("DELETE")) {

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    Message msgToSend = msgs[0];
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgToSend);
                    out.flush();
                    out.close();
                    socket.close();
                } else if (messageType.equalsIgnoreCase("RECOVERRESPONSE")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    Message msgToSend = msgs[0];
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgToSend);
                    out.flush();
                    out.close();
                    socket.close();
                } else if (messageType.equalsIgnoreCase("ALIVE")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(m.destport));
                    Message msgToSend = msgs[0];
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(msgToSend);
                    out.flush();
                    out.close();
                    socket.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "CLIENT! Exception encountered");
                isdead.put(msgs[0].key, true);
            }
            return null;
        }
    }

    private class ClientTaskQ extends AsyncTask<Message, Void, Boolean> {

        @Override
        protected Boolean doInBackground(Message... msgs) {
            try {
                Message m = msgs[0];
                String mport = m.destport;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mport));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(m);
                out.flush();
                out.close();
                socket.close();

                flag.put(m.key, true);
                if (m.count.equalsIgnoreCase("1")) {
                    while (flag.get(m.key)) {
                    }
                };
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "Exception encountered HEREEEEEEEEEEE");
                isdead.put(msgs[0].key, true);
            }
            return false;
        }
    }
}
