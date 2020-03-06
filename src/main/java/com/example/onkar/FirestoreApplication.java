package com.example.onkar;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.jcabi.xml.XML;
import com.jcabi.xml.XMLDocument;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FirestoreApplication {
    public static Firestore databaseClient = null;
    public static List<Map<String, String>> documentsList = new ArrayList<Map<String,String>>();
    public static List<String> documentIDs = new ArrayList<>();
    public static void main(String[] args) {

        GoogleCredentials credentials = null;
        try {
           credentials  = loadGoogleCredentials();
           createFirestoreClient(credentials);
        } catch(Exception e) {
            e.printStackTrace();
        }



        try {
            createDocumentsAndSaveDocumentIDs();
            writeDocumentsToFirestore();
            readDocumentsFromFirestoreBasedOnDocumentID();
            readAllDocumentsFromFirestore();
        }
         catch(Exception e) {
            e.printStackTrace();
         }
    }

    private static void readAllDocumentsFromFirestore() {

        try {
            long startTime = System.currentTimeMillis();
            List<QueryDocumentSnapshot> documents =  databaseClient.collection("delivery_collection").get().get().getDocuments();
            int count = 0;
            for(DocumentSnapshot snapshot : documents) {
                System.out.println("Fetched Document from Firestore with ID : "+snapshot.getId());
                count++;
            }
            System.out.println(" No of Documents are : "+count);
            long endTime = System.currentTimeMillis();
            double totalTIme = (double) (endTime - startTime)/1000;
            System.out.println(" Time to Fetch all documents : "+totalTIme);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


    private static void readDocumentsFromFirestoreBasedOnDocumentID() {
        documentIDs.stream().forEach( documentID -> {
            try {
                long startTime = System.currentTimeMillis();
                databaseClient.collection("delivery_collection")
                        .whereEqualTo("documentText",documentID)
                        .get().get().getDocuments().forEach(document -> {
                            String documentId = document.getId();
                            if(documentId.equals(documentID)) {
                                System.out.println("Found the Document");
                            }

                });
                long endTime = System.currentTimeMillis();
                double totalTIme = (endTime-startTime)/1000;
                System.out.println("  Time to retrieve Document with ID : "+documentID + " is : "+totalTIme);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }


    private static void writeDocumentsToFirestore() {
        System.out.println(" Start Time Writing Data : "+System.currentTimeMillis());
        documentsList.parallelStream().forEach( mapData -> {
            String documentId = mapData.keySet().stream().map( key -> key).collect(Collectors.joining());
            System.out.println("Writing Document with Id : "+documentId);
            String value = mapData.get(documentId);
            mapData.clear();
            documentIDs.clear();
            if(mapData.isEmpty()) {
                System.out.println("Rewrite  Map");
                mapData.put("documentText",value);
            }

            ApiFuture<WriteResult> future =  databaseClient.collection("delivery_collection").document(documentId).set(mapData);

            try{
                System.out.println("Document with DocumentID : "+documentId + " Written Time : "+future.get().getUpdateTime()+ " Is Done? - "+future.isDone());
            } catch(Exception e) {
                e.printStackTrace();
            }

        });


        System.out.println(" End Time of writing data : "+System.currentTimeMillis());
    }


    public static void createDocumentsAndSaveDocumentIDs() throws ParserConfigurationException, IOException, SAXException {

        FirestoreApplication application = new FirestoreApplication();
        File file = new File("/Users/onkar/IdeaProjects/Firestore/src/main/resources/ISOM.xml");
        XML xml = new XMLDocument(file);
        String xmlContent = xml.toString();
        System.out.println(xmlContent);
        Random random = new Random();
        documentsList.clear();
        for(int i =0;i< 100;i++) {
            long longDocumentID = random.nextLong();
            Map documentData = new HashMap<String, String>();
            documentData.put(String.valueOf(longDocumentID),xmlContent);
            documentsList.add(documentData);
            documentIDs.add(String.valueOf(longDocumentID));
        }
    }


    public static GoogleCredentials loadGoogleCredentials() throws IOException {
        InputStream serviceAccount = new FileInputStream("/Users/onkar/Documents/Firestore_ServiceAccountKey.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
        return  credentials;
    }



    public static void createFirestoreClient(GoogleCredentials credentials) {
        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(credentials)
                .build();
        FirebaseApp.initializeApp(options);
        databaseClient = FirestoreClient.getFirestore();
    }


}
